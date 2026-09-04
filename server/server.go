package server

import (
	"fmt"
	"log/slog"
	"strconv"

	"time"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"github.com/VictoriaMetrics/metrics"
)

type Response struct {
	Granted int32
}

type Processor interface {
	Request(key string, amount int32) Response

	//Relies on no concurrent/subsequent calls to request()
	//(if that is idiomatic expectation then we don't need that comment)
	Close() //close is the idiomatic name for stopping lifecycle and releasing resources
}

type Config struct {
	Port     	int           				`yaml:"port"`
	MaxRequest  int32         				`yaml:"max_requests"`
	APIs 		map[string] int32			`yaml:"api"`
}

type Bucket struct {
	Count int32
	Limit int32
	StartedAt time.Time
	Issued int64
}

//TODO: how critical can possible time leap be? Like, in "leap second" or "switch to daylight time"
func Refill (bucket *Bucket, now time.Time) {
	//Have to re-establish some type boundaries to avoid precision loss (= increment in stairs) 
	// by accidentally casting float64(int64) when I wanted to cast float64(int32); or to avoid
	// messing up integer conversion.
	var limit int32 = bucket.Limit

	//Actually we don't need utmost precision here, if we pour less buckets this microsecond, we'll just pour more the next one;
	// we only want it to be more or less smooth, so millis would not work good. At the same time, why lose precision by using micros
	// when we can just as well use nanos? Even 100 years as Nanos still fits int64
	elapsed := now.Sub(bucket.StartedAt).Nanoseconds()
	expectation := float64(elapsed) / float64(time.Second.Nanoseconds()) * float64(limit)
	delta := int64(expectation) - bucket.Issued
	bucket.Issued += delta

	//Now put them to bucket. But, we don't necessarily put all of them - to avoid bursts after delays
	//This also helps us avoid >int32 deltas
	if delta > int64(limit) {
		delta = int64(limit)
	}
	bucket.Count += int32(delta)
	if (bucket.Count > limit) {
		bucket.Count = limit
	}
}

//TODO better move to main or somewhere else
func CreateHTTP(processor Processor, fiberFailed chan<- struct{}, myMetrics *metrics.Set) *fiber.App {
	handlerTime := myMetrics.NewHistogram("handler_time")

	app := fiber.New()

	app.Use(recoverer.New(recoverer.Config {
		PanicHandler: 
			func(c fiber.Ctx, r any) error {
				fiberFailed <- struct{}{}
				//Since panics are worse situation in Go than say Java exceptions, we don't look at the error and deduce what
				// happened and what details to give our client; we don't give any
				return fiber.ErrInternalServerError
			}}));

	//Actually, different buckets could be served by different dedicated workers, especially since there are no causality requirements
	// between requesting from them. Instead, we'll keep serving them all from same thread, to avoid overcomplicating this
	// PoC work, because otherwise we need to consider it carefully (also we could serve different APIs from different shards).
	handler := func (key string, amount int32, c fiber.Ctx) error {
		start := time.Now()
		slog.Debug("Serving request", "key", key, "amount", amount)
		result := processor.Request(key, amount)
		handlerTime.UpdateDuration(start)
	
		if result.Granted == amount {
			return c.SendStatus(fiber.StatusOK)
		} else {
			//TODO headers like x-ratelimit-*
			c.Set("X-RateLimit-Granted", strconv.Itoa(int(result.Granted)))
			return c.SendStatus(fiber.StatusTooManyRequests)
		}
	}
	
	//Use POST, not GET, because of read-only and idempotency requirements of HTTP, and because request body allows to send more data and in a safer way.
	//TODO better move parameter to POST body
	app.Post("/:key", func(c fiber.Ctx) error {
		q, err := strconv.ParseInt(c.Query("q", "1"), 10, 32)
		if err != nil {
			return fmt.Errorf("Amount parse error: %w", err)
		}
		return handler(c.Params("key"), int32(q), c)
	})
	//However, for debugging purposes GET is sometimes more convenient, while under heavy development
	app.Get("/:key", func(c fiber.Ctx) error {
		q, err := strconv.ParseInt(c.Query("q", "1"), 10, 32)
		if err != nil {
			return fmt.Errorf("Amount parse error: %w", err)
		}
		return handler(c.Params("key"), int32(q), c)
	})

	return app
}

//TODO better move to main or somewhere else
func StartHTTP(http *fiber.App, fiberFailed chan<- struct{}, cfg *Config) {
	go func() {
		defer func() { //always report termination
			//We still want to attempt a controlled termination in main routine, not just crash the app right here
			if r := recover(); r != nil {
        	    slog.Error("HTTP server panicked", "error", r)
				fiberFailed <- struct{}{}
        	}
		} ()
			
		//startup errors return non-nil, graceful shutdown returns nil, shutdown errors are only returned via shutdown() - not here
		if err := http.Listen(fmt.Sprintf(":%d", cfg.Port)); err != nil {
			slog.Error("HTTP server startup failed", "error", err)
			fiberFailed <- struct{}{}
		}
	}()
}

//TODO: mutex + fast path (or maybe CAS)
//TODO: config for acceptance limit Q (max sum quota) and acceptance multiplier K; load shedding over capacity = L * K
//      (google's default is K=2.0, provided that rejection path is much shorter than acceptance path, but I'll need to measure
//       whether that's my case or not, and maybe set it to some LOWER value - after all, by serving over Q, we only want
//.      to propagate feedback to clients, we don't necessarily need to serve those extra requests out of pure principle;
//.      thus, if that's too costly, we are going to just ignore them).*
//.      * Setting the capacity closer to Q, however, means load-shedding is more likely to happen, and it can hurt well-behaving
//.        clients; that's bad, but if it's good enough compromise for Google, I'll take it. 
//         Thus, picking good value for K must be tuned for each specific system, minding how compliant the clients are expected to be. 

/*
type ComplexRequest struct {
    UserID int32  `uri:"id"`        // Extracted from path: /users/:id
    Search string `query:"search"`  // Extracted from query string: ?search=abc
    Role   string `header:"X-Role"` // Extracted from HTTP headers
}
*/

//wget https://storage.googleapis.com/hey-releases/hey_linux_amd64
//chmod +x hey_linux_amd64
//sudo mv hey_linux_amd64 /usr/local/bin/hey
//hey -n 100000 -c 2 -m POST "http://localhost:3000/api1"

// Первые тесты делал без полезной нагрузки - просто возращал ОК.

// Деградацию правильно мерить не по тому что длительность теста не меняется
// ведь клиенты не обязательно ложатся в гэпы друг друга если только перекрытие не многократное
// например он мог принять все запросы одного и положить подряд - и лишь за ними запросы другого
// и тогда запросы другого все лягут в очередь и все сдвинутся на некий интервал
// Ну не буквально так в нашем случае, но в общем случае если мы приближаемся к лимиту но еще не достигли - 
// они могут и мешать друг другу, как мне кажется, и это нормально
// правильнее замерять чисто по рпс: пока рпс масштабируется линейно с добавлением клиентов - значит сервер не достиг своей capacity

// А есть ли разница от масштабирования vCPU сервера? Ну для чистого сервера есть, а конкретно для моей горутины если она все в один
// поток роутит - скорее нет. 
// Но для сервера есть =) А если дать ему скажем 16 потоков, из них 12 под Го и 4 оставить системе?

// Я тестировал на сервере в 20 vCPU и двух клиентах по 6 ядер.
// Вот сейчас 2x6+20 на 2x32 дали 38+38k, а один дает 47k - есть деградация
// 2x24 дали 34+34 а один дает 41
// даже 2x16 уже дает 2x26 вместо 2x33 :(
// Почему деградация? Я замерял ранее на 8-ядерных клиентах с теми же параметрами и вроде бы не было деградации на таких
// низких параметрах.

// Эврика. Снизив число ИСПОЛЬЗУЕМЫХ ядер до 4 на сервере (именно GOMAXPROCS) - я неожиданно повысил производительность так
// что почти нет деградации =)))
// Для 32+32 потока (с двух клиентов) получилось теперь 45к+45к )))) почти без деградации (один клиент выдает 39к)
// Во что же упираемся? Если с 4 ядрами производительность хуже чем со всеми 20 или 16.
// Проверил теорию - подсказанную ИИ - что много уходило на конкуренцию и нужно включать в fiber режим prefork.
// Мне он не очень подходит - ведь это независимые инстансы а у меня нет синхронизации состояния между ними;
// но для целей проверки причин сойдет и так.
// Проверил: с префорком и без GOMAXPROCS (иначе он на все инстансы распространится) два клиента по 32 потока
// дали вместо 47k - 38+38. Это медленнее чем без префорка =)

// Следующая теория была - тоже подсказанная ИИ - что конкретно для префорка при 16 ядрах невыходно иметь всего лишь 2
// клиента по 10 конекшенов, происходит недонасыщение некоторых. Переделал тест на
// hey -n 100000 -c 128 -q 400 -m POST "http://84.201.158.125:3000/api1"
// И получил на 32+32 47+47K без деградации (один инстанс тоже дал 47к) 
// Затем -q 500 попробовал и получилось 56k и 53+53K :)
// -c 256 -q 350 => 55+58 против 61k
// Избавились от недонасыщения и получили 110К - но это такой же результат какой был без префорка =)

// Далее проверял гипотезу что просто сеть слишком медленная мне досталась, но тест показал 2гб/с. 
// Но дальше ИИ предположил, что отдельные ограничения выданного мне сетевого интерфейса на самом деле зависит от числа ядер,
// дескать это обычная картина в облаке, и в Яндекс в частности. Я конечно никогда с публичными облаками не работал и не мог такого 
// предположить, и вот похоже что так и есть: если для 20 ядер потолок был 110к, то для 24 ядер уже 124к, а когда сделал 32 ядра
// и заодно вместо Cascade Lake - Ice Lake, получил аж 150К!
// Так что
// 1) гипотеза о зависимости производительности сети от мощности машины подтверждается - но неясно, то ли дело в облаке, то ли 
//    просто прием такого количества соединений требует много процессорных мощностей. Но вроде бы нет: график мониторинга загрузки CPU
//    показывал около 20%.
// 2) но GOMAXPROCS=4 на этой машине тоже достиг 150К
// 3) я делаю предварительный вывод, что prefork не имеет смысла использовать, пока не достигнут естественный предел мощностей 
//    одного инстанса (без форков), так как работа в один инстанс по какой-то причине эффективнее. А вот когда предел достигнем, 
//    дальнейшее увеличение CPU (или улучшение сети) уже не будет помогать, и тогда спасает prefork. 
// 4) нащупать этот предел я пока не могу, так как у меня маленькое и недорогое облако. Зато я могу принять решение, что
//    так как мы знаем что даже для 150К fiber без полезной нагрузки не вносит сам по себе нелинейность (=справляется быстро),
//    то на меньших нагрузках (100-150к) это тоже верно. Любая нелинейность будет обусловлена именно полезной нагрузкой.
// 5) значит будем просто тестировать на 100-150к уже с полезной нагрузкой и иcкать, где возникнет нелинейность.

// Я в итоге вообще взял один клиент 20 vCPU Ice Lake - и сервер такой же. В клиента еще добавил те же оптимизации для
// сети на всякий случай, что и на сервер (maxconn и прочее)
// А также тоже поставил ряд экспериментов сколько лучше всего ядер дать hey (благо он на Го) - аналогично серверу тут оказалось
// что больше не значит лучше.
// Теперь единственный инстанс клиента выдает почти 120К!
// GOMAXPROCS=7 hey -n 200000 -c 512 -q 300 -m POST "http://89.169.146.102:3000/api1"
// Вот на этом и будем тестировать.

// Добавил на сервер полезную нагрузку, и теперь при клиенте MAX=7 производительность на сервере упала, уже не 120к, 
// но все равно 90-100к в основном.
// А если
// 6x128x300 => 33k (6 - это GOMAXPROCS конкретного теста), а два таких параллельно в одном инстансе -> 66k (нет деградации на сервере)
// GOMAXPROCS=6 hey -n 50000 -c 128 -q 350 => 44Kx2, нет деградации
// GOMAXPROCS=6 hey -n 50000 -c 128 -q 370 => 43+46, вместо 2x47, пошла деградация - но мы знаем что она не из-за ограничений сервера
// и скорее всего не из-за клиента (выше показано что и тот и другой были способы обрабатывать больше - при -c 512 -q 300; хотя конечно
// само нарастание конкуренции на клиенте могло начать приводить к перегибу - будем и такую возможность держать в уме, может быть потом
// перепроверим на более мощном клиенте).

// Вот тут и есть наш излом. И что делать с этим? Ну, надо например посмотреть нельзя ли так переписать код
// или изменить подход, что деградация при этом показателе пропадет.

// Посмотрим теперь на наши счетчики
// Stopping totalP=51 totalH=1263
// То есть - 51 мс проведена в самом процессинге, и 1.2 секунды проведены в хэндлере (но конечно параллельно)
// Длительность теста составила 1.1 примерно, что означает что суммарное время в хэндлере с ожиданием - лишь немного больше времени теста.
// Наверное это значит как раз что запросы почти не ждут из-за превышения капасити, 
// а ждут только передачи данных (=и по той же причине почти нет искажений линейности)

// Но тогда кажется что такое время ожидания очень велико
// К тому же мы не знаем, это же среднее, может на самом деле оно часто нулевое
// Надо бы эту теорию проверить - прикрутил victoria metrics ради готовой гистограммы вместо суммы totalH
// p50 (Median): 2.45 µs (50% of your requests finish in under 2.45 µs).
// p90: 40.84 µs (90% of requests finish in under 40.84 µs).
// p95: 68.13 µs (95% of requests finish in under 68.13 µs).
// p99 (Tail Latency): 146.80 µs (Only 1% of total requests take longer than this threshold).
// Max = 879.9 µs
// То есть я частично прав, половина запросов отрабатывают за 2.5 микросекунды. Но это все равно при умножении
// на 50к будет в разы больше чем 51 мс (чистое время процессинга). В разы - но не на порядок. То есть существует какой-то
// fast-path, но он реализуется лишь иногда при такой нагрузке.
