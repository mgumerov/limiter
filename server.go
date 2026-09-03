package main

import (
	"context"
	"log/slog"
	"time"
	"fmt"
	"bytes"
	"strconv"
	"sync/atomic"

	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"gopkg.in/yaml.v3"

	"github.com/VictoriaMetrics/metrics"

)

type Request struct {
	key string
	amount int32
	reply chan<- Response
}

type Response struct {
	granted int32
}

type Processor struct {
	rqChan chan<- Request
}

var myMetrics = metrics.NewSet()
var handlerTime = myMetrics.NewHistogram("handler_time")

type Config struct {
	Port     	int           				`yaml:"port"`
	MaxRequest  int32         				`yaml:"max_requests"`
	APIs 		map[string] int32			`yaml:"api"`
}

func main() {
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		var logLevel slog.Level
		if err := logLevel.UnmarshalText([]byte(level)); err != nil {
			slog.Warn("Invalid log config specified, ignoring")
		} else {
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{ Level: logLevel })))
		}
	}

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		slog.Error("failed to load configuration", "path", configPath, "error", err)
		os.Exit(1) //TODO
	}

	processorFailed := make(chan struct{}, 1)
	var processor *Processor = startProcessor(processorFailed, cfg);

	//The remainder of this function could be just fiber.Listen(), but Fiber v2 does not listen context for graceful
	// shutdown, neither does it return control after inialization. So, decided to move its startup into a goroutine
	// completely and provide means for watching for subsequent failure of this or other subsystems.
	//But, fiber V3 actually knows how to listen for graceful shutdown; meaning, if we accept 
	// it as our only "blind-launch" but critical service, we could skip it all and just fiber.Listen()
	// right here, since we won't be needing to wait for some other events in parallel.
	//In the end, I decided to take the longer route because I want to be able to use other engines than Fiber,
	// so I don't want it to transform my main routine into Fiber's event loop. Besides, this requires me to
	// learn how to cope with related problems.

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM, //sadly no platform-neutral os.* constant for this; even though actually Go translates to SIGTERM on Windows
	)
	defer stop() //we won't need this context after this function completes

	//Idiomatic approach says passing contexts along (here, or/and to fiber.Listen) because this clearly involves some 
	// i/o and long activity. But actually it depends on how the called func will use the context: maybe I am passing 
	// request-scoped context to some async processing, for example. In this case, passing globally-scoped notify-context 
	// actually makes sense, but only if I want Fiber to use it for graceful shutdowns, and I stated above that I prefer it not to.
	fiberFailed := make(chan struct{}, 1)
	fiber := createHTTP(processor, fiberFailed)
	startHTTP(fiber, fiberFailed, cfg);
	
	select {
	case <- ctx.Done():
		//do nothing
	case <- fiberFailed:
		slog.Error("HTTP server terminated unexpectedly") //since we did not tell it to shut down yet
		fiber = nil
	case <- processorFailed:
		slog.Error("Processing Worker terminated unexpectedly") //since we did not tell it to shut down yet
		fiber = nil
	}
	
	slog.Info("Stopping", "totalP", totalP.Load() / 1000000)
	var buf bytes.Buffer
	myMetrics.WritePrometheus(&buf)
	slog.Info("Handler time", "metrics", buf.String())
	
	if fiber != nil {
		if err := fiber.Shutdown(); err != nil {
			slog.Error("Error while shutting down HTTP server", "error", err)
		}
	}
	processor.close()
}


type Bucket struct {
	count int32
	limit int32
	startedAt time.Time
	issued int64
}

var totalP atomic.Int64

// In worker-pool approach, worker goroutines never complete, but they are reused and that saves some allocations etc.
// In semaphore-protected approach, answering routines are completed and cleanly recreated, that's cleaner
//  (no need to guarantee goroutine will not die out of panic) but more expensive.
// Also, since I don't want more than 1 request processed at each moment, I can use mutex-protected approach. 
//  It does not use allocations and does not need goroutine creation at all, and appears like most efficient solution.
// What do I pick? I'll allow selecting between worker-pool and mutex, because I have some reservation about mutex drawbacks 
//  and need to do measurements.
func startProcessor(processorFailed chan<- struct{}, cfg *Config) *Processor {
	reqChan := make(chan Request) //unbuffered, because what good such buffering is - under heavy load?
	
	//Process all requests sequentially by a single goroutine
	go func() {
		defer func() {
			//We still want to attempt a controlled termination in main routine, not just crash the app right here
			if r := recover(); r != nil {
				slog.Error("Processing Worker panicked", "error", r)
				processorFailed <- struct{}{}
			}
		}()
		
		buckets := make(map[string]*Bucket)
		startedAt := time.Now()
		for key, limit := range cfg.APIs {
			//We could start with startedAt=0, but then one of two things happen
			// - either first request interpretes that 0 as "empty bucket" - i.e. bucket starts refilling only then (so some time is lost)
			// - or maybe as "full bucket" - bad if service is restarted from empty bucket and restart took less than 1 second (so, extra request may pass through)
			//Also we start with empty buckets because of that same problem with "full bucket" approach
			buckets[key] = &Bucket { limit: limit, startedAt: startedAt }
		}

		//Note, range loop, just like 2-argument reading form, checks for closing the channel,
		// we'll use it to propagate graceful shutdown to our goroutine
		for request := range reqChan {
			start := time.Now()
			slog.Debug("Processing request", "key", request.key, "amount", request.amount)

			//Compared to my initial implementation, keeping buckets in heap (presumably) and retrieving potentially
			// new bucket each time - hurts locality. It might make sense to associate dedicated worker with each
			// bucket and make that bucket a local variable, that way maybe compiler will even use registers and not
			// real RAM. On the other hand, it's not like we use DIFFERENT workers to process same bucket, it's 
			// the other way around, so it should not hurt that much.
			// I did some *load tests* and they don't show significant differences. Actually, they weren't stable enough
			// to prove there is no difference, but in both cases pure processing time (not counting receiving from channel)
			// for 500k requests with pauses makes up for 140-150ms. So, further measurements would be necessary, but 
			// there is no immediately noticeable difference. More importantly, since I am aiming at 500k RPS
			// and currently only seem to serve 100K, clearly 140ms or say 120 or 160ms do not really mean any difference,
			// it's not the slowest thing on request path :(
			bucket, ok := buckets[request.key]
			if (!ok) {
				request.reply <- Response { granted: 0 } //for real use, we should maybe distinguish this error reason
				continue
			}

			refill(bucket, time.Now())

			if (request.amount > cfg.MaxRequest) {
				request.amount = cfg.MaxRequest
			}
			granted := min(request.amount, bucket.count)
			bucket.count -= granted
			request.reply <- Response { granted: granted }

			totalP.Add(time.Since(start).Nanoseconds())
		}
	}()

	return &Processor { rqChan: reqChan }
}

//TODO: how critical can possible time leap be? Like, in "leap second" or "switch to daylight time"
func refill (bucket *Bucket, now time.Time) {
	//Have to re-establish some type boundaries to avoid precision loss (= increment in stairs) 
	// by accidentally casting float64(int64) when I wanted to cast float64(int32); or to avoid
	// messing up integer conversion.
	var limit int32 = bucket.limit

	//Actually we don't need utmost precision here, if we pour less buckets this microsecond, we'll just pour more the next one;
	// we only want it to be more or less smooth, so millis would not work good. At the same time, why lose precision by using micros
	// when we can just as well use nanos? Even 100 years as Nanos still fits int64
	elapsed := now.Sub(bucket.startedAt).Nanoseconds()
	expectation := float64(elapsed) / float64(time.Second.Nanoseconds()) * float64(limit)
	delta := int64(expectation) - bucket.issued
	bucket.issued += delta

	//Now put them to bucket. But, we don't necessarily put all of them - to avoid bursts after delays
	//This also helps us avoid >int32 deltas
	if delta > int64(limit) {
		delta = int64(limit)
	}
	bucket.count += int32(delta)
	if (bucket.count > limit) {
		bucket.count = limit
	}
}

//Although idiomatic (responding via channel), implementation is still better be hidden
func (p *Processor) request(key string, amount int32) Response {
	//buffered, because it makes no sense to block when responding, however little are chances;
	// also, this avoids depending on the receiving side _still being alive_ (might have panicked or whatever)
	//TODO potentially a performace hindrance, say 1M allocations/second, need to somehow measure and try another approach
	reply := make(chan Response, 1)
	p.rqChan <- Request { key: key, amount: amount, reply: reply }
	return <- reply
}

//Relies on no concurrent/subsequent calls to request()
//(if that is idiomatic expectation then we don't need that comment)
func (p *Processor) close() { //close is the idiomatic name for stopping lifecycle and releasing resources
	close(p.rqChan)
}

func createHTTP(processor *Processor, fiberFailed chan<- struct{}) *fiber.App {
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
		result := processor.request(key, amount)
		handlerTime.UpdateDuration(start)
	
		if result.granted == amount {
			return c.SendStatus(fiber.StatusOK)
		} else {
			//TODO headers like x-ratelimit-*
			c.Set("X-RateLimit-Granted", strconv.Itoa(int(result.granted)))
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

func startHTTP(http *fiber.App, fiberFailed chan<- struct{}, cfg *Config) {
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

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	//This is better be decoupled from reading because we could load config from different sources. For now, we don't.
	if cfg.MaxRequest == 0 {
		return nil, fmt.Errorf("Maximum request size is not defined")
	}
	if cfg.Port == 0 {
		return nil, fmt.Errorf("Port is not defined")
	}

	return &cfg, nil
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
// GOMAXPROCS=6 hey -n 50000 -c 128 -q 370 => 43+46, вместо 2x47, пошла деградация

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
