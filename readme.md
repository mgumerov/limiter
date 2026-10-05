# Research project: Centralized Rate-Limiter

## Motivation

I was going to study advanced aspects of Go in a little Senior level project. One of AI's proposal was a rate limiter like the built-in one, but I judged that an internal rate limiter is a very narrow task and hence offers little in terms of gaining/showing proficiency in different areas. So I went for external, centralized rate limiter, the way some use Redis for example.

The code might be a little hard to read because it is unusually densely commented: in this project I tend to document most of reasons/decisions in comments.

## Features / Goals

- This limiter is supposed to be used as outgoing rate limiter, not incoming rate limiter, which reflects on some decisions made,
like whether to allow exceeding of a limit, or how malicious our clients might be, etc.
  - This also affects the expected profile of connections. In incoming requests we would more likely see lots of clients with just one or a few connections, whereas an outbound limiter will see not so many clients but lots of connections (for HTTP 1; obviously for HTTP/2 it will be different)

- The client calls the limited to request a token, a success means that the caller can make a request to a target API; a failure means the request is rejected, most probably because the request to target API would exceed the RPS limit
- The caller can request multiple tokens at once, but I did not yet consider noisy-neighbour fairness implications in this case. It was simply more interesting technically to find a way to serve super high RPS.
- It sets a dedicated limit for each of unique API keys, and every request for tokens specifies an API key
- It prioritizes limiting guarantees over throughput: for example, if no tokens were requested for a second, it does not mean a client can request twice the RPS limit the next second, even it means that means overall RPS over time will be less than the limit.
- It currently uses one bucket-based algorithm but in three flavours: by-the-book one, with requests to same API key serialized by using  channels and a worker goroutine (more go-like supposedly), or by using Mutex (better performance); and custom one, of my design, which does not require serialization and relies on CAS operations instead. The algorithm can be picked via env vars; other options are taken from config.yaml
- It does not try to be scalable through adding extra instances (well, of course it still can be sharded by API keys, but barring that - one API key cannot be served by multiple instances simultaneously)
- Instead, it tries to provide extremely high performance with just single instance; my latest measurements say 5M+ RPS beyond the HTTP server boundary. Sadly, at the time I do not have the proper hardware to test end-to-end performance (including network overhead + HTTP server), I did observe 120K+ RPS in the cloud but I will probably see much more on good machines connected by fast LAN.
- It currently accepts HTTP requests using Fiber server because of its proven high performance. I am also planning to add serving GRPC as an option, to see if multiplexing via less connections will also provide comparable performance.
- It can still use multiple instances - not for scalability but for availability. When used this way, it uses etcd to make sure at most one instance can serve requests at any time, even in case of network partitioning.
- It publishes some Prometheus metrics to allow observing its statistics; however it cannot publish too much details because gathering them would introduce extra contention and damage the performance.
- Also carries gRPC server as an option

## Next steps:
- Use fast LAN and two good machines to get big enough load to see Fiber's limits

# Log of testing (in Russian):

1. Cloud based

//wget https://storage.googleapis.com/hey-releases/hey_linux_amd64
//chmod +x hey_linux_amd64
//sudo mv hey_linux_amd64 /usr/local/bin/hey
//hey -n 100000 -c 2 -m POST "http://localhost:3000/api1"

// Первые тесты делал без полезной нагрузки - просто возвращал ОК.

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

// Провел тест реализации на мьютексах. Значимых отличий по линейности не увидел, так что время ожиданий получается такое же :(
// Но!
// P95 теперь ниже 2мкс!
// P99 - 21 мкс
// MAX в районе 800мкс

// Но тогда у меня вопрос =))) а что тогда тормозит? =) Я еще мог поверить что 50% медленных запросов создают общее время выполнения
// теста более 1 сек, но теперь-то медленных запросов в 100 раз меньше чем быстрых!
// Неужели бОльшую часть задержек создает сеть?
// Это бы объяснило скорость теста (если всякий раз клиент ждет ответа и лишь потом запускает новый запрос)
// но не объясняет перегиб на сервере. Почему сервер-то не может больше обработать?

// Хм ну вот я делаю 300 запросов и 1 конекшен - это занимает 1 секунду!
// Но тут все ясно, один поток последовательно не может запустить больше, это ограничение сети или интерфейса
// 10 потоков ровно так же за секунду отрабатывают и 100 тоже
// но вот 1000 потоков уже 2.5 сек
// 500 потоков чуть более 1 сек
// 250 потоков - без перегиба тоже 1 сек

// Далее не важно - то ли добавить еще 250 конекшенов то ли второй такой инстанс запустить - все равно 500 уже уходит за 1 сек
// Но возьмем вот этот вариант с 250
// Почему такой тест занимает 1 сек?
// Не потому что на обработку 75к запросов нужно 1 сек
// А потому что каждый из 250 клиентов не может слать запросы быстрее!

// Аааа блин я ж сам ему сказал - шли в секунду 300 запросов =) Он никак не может работать быстрее чем за 1 сек =)))
// (Правда остается вопрос почему излом на сервере)
// Что если сказать больше?

// начиная с 1000 запросов на конекшен - он уже захлебывается
// 750 - нормально
// и 7500 с 10 клиентами - тоже линейно
// 75к со 100 клиентами - уже нелинейно (немножко)
// GOMAXPROCS=7 hey -n 75000 -c 100 -q 750 -m POST "http://51.250.88.215:3000/api1"
// Но мы помним что сервер без нагрузки линейно принимал до 88к/сек при такой конфигурации

// Итак, линейность при 44+44 мы все еще имеем, а при бОльших все еще нет - но это уже может упираться в сеть
// Причем тесты показали сейчас, что без полезной нагрузки (No-op processor) и с ней предел примерно одинаковый :(
// Невозможно выбирать лучшее решение, если сеть не дает нагрузить как следует.
// Хотя можно по гистограмме ориентироваться (например что 95% запросов или только 50% выполняются быстрее 10 мкс)
// но это не совсем четкий пруф как оно себя поведет в реальной работе.

2. Internal tests

// Значит перейдем на внутренние самотесты
// исключая сеть
// возможно вообще уберем из уравнения hey и fiber и будем напрямую вызывать хендлеры из разных потоков
// жаль только что это создаст эффект самоглушения - конкуренции за ресурсы
// или не создаст?
// там конкуренция была из-за общего сетевого стека а тут общий будет только CPU
// если мы скажем выделим ровно N машин под лимитер и ровно M под запуск тестов то мешать не будут (или минимально)
// но возникнет вопрос как один процесс будет дергать второй без сетевого стека?
// А нужно ли два процесса?
// - Хендлер выполняется в той же рутине которая вызвала хэндлер, правда мы не знаем какое
// количество горутин файбер выделяет под это, чтобы выделить столько же. А так - если мы просто на верхнем уровне каждой горутины
// сделаем цикл и будем вызывать хендлер (не через сеть), это не сворует у сервера практически никаких ресурсов кроме лишнего Call.
// Второй процесс не нужен. Но надо бы понять как реалистично сделать столько же горутин как в файбере.

// А все просто. У нас же HTTP 1.1 пока что, и без пайплайнинга (а если и с ним - файбер обрабатывает запросы последовательно),
// поэтому fiber обслуживает каждый коннекшен одной горутиной, и так как мы знаем из настройки hey сколько коннекшенов использовать,
// то знаем сколько горутин использовать. Остается в них добавить такую же логику лимитинга простенькую чтобы равномерно раскидывать,
// как в hey (да или просто как в моем же алгоритме!), и все.

// Можно даже готовую либу вроде F1 использовать, потому что тогда весь тест это 10-liner который к тому же умеет
// брать параметры запуска из командной строки
// Вот я прогнал локальные тесты как делал удаленно - скажем (2x128)x350 - это прицел в 88k запросов в секунду
// P=M S=T go run . run constant tests --rate 88000/s  --concurrency 256
// => p50 = 375 нс (!), p90 = 25 мкс, p97 = 1.2 мс, p99 = 2.6 мс, max= 7 мс
// Далее эту нагрузку УДВОИЛ и все равно за 992 мс выполнил 176к запросов,
// => p99 = 3мс, max = 15 мс, а в целом так же
// 350K => теперь уже max=12 мс, в остальном распределение то же
// 700K => аналогично
// 3М это максимум который я успеваю уложить в 1с (предел линейности = capacity), и на нем
// => p50 = 208 нс (все еще!), p90 = 6 мкс (все еще!), p97 = 755 мкс, p99 = 2 мс, max = 11 мс (в сущности хуже не стало!)

//Ну это было с M процессором, теперь нужно сравнить с W - сколько успеет разгрести он
//P=W S=T go run . run constant tests --rate 350000/s  --concurrency 256
//=> P50 = 228 mks, P90 = 300 mks, P97 = 363 mks, P99 = 628 mks, max = 1.5 ms
//P=W S=T go run . run constant tests --rate 2000000/s  --concurrency 256
//=> P50 = 99 mks, P90 = 147 mks, P97 = 195 mks, P99 = 406 mks, max = 820 mks
//Но это предел. Больше 2 млн не успеваем (пробовал concurrency=512, не лучше)
//Пока неясно, почему - возможно (было бы логичнее всего) из-за тяжелого начала распределения, все-таки тут P50 в 100-1000 раз тяжелее
// чем с P=M
//Кстати я пробовал убрать из W-процессора выдачу processing_time, вдруг она добавляет время на синхронизацию - нет, не повлияло

//Итого
// 1) пока с большим отрывом впереди вариант с Mutex, хоть он и теряет локальность и выглядит не идиоматично;
//    (сейчас и в Worker не полная локальность; впрочем ранее я пытался добавить хак - вернуть больше локальности, один локальный бакет,
//	   но конечно это не сказалось: куда больше времени проводится в handler_time чем в processing_time, поэтому ускорение процессинга не так
//    важно, как важна возможность уменьшить контеншен или уменьшить ожидание при контеншене, а на это локальность не повлияла)
// 2) на самом деле оба варианта справляются с 2M-3M rps, то есть целевые показатели я перекрываю кратно, упираясь только в слой http-сервера;
//    а вот достичь его пределов прочности пока не получилось: на локальной машине это не проверишь нормально, а в облаке я уперся в лимиты,
//    не готов платить за супер-машину. Уж лучше отсыпать денег за thunderbolt кабель, он хоть у меня останется.

//Дальше надо попробовать написать вариант на CAS вместо мютекса
//Итак, с P=M:
//2026/09/07 17:18:34 INFO Stopping metrics="handler_time{quantile=\"0.5\"} 2.08e-07\nhandler_time{quantile=\"0.9\"} 1.6834e-05\nhandler_time{quantile=\"0.97\"} 0.001687792\nhandler_time{quantile=\"0.99\"} 0.004024625\nhandler_time{quantile=\"1\"} 0.027243875\nhandler_time_sum 428.26521755474727\nhandler_time_count 3000000\n"
//И все, больше не успеваем.
//Теперь с P=C.
//Сначала уперся тоже в 3M. Возникли подозрения что предел обусловлен чем-то другим, например - сбором статистики. Отчасти это подтвердилось,
//удалось его ускорить и получить с пустой обработкой реквеста - 9М. То есть теперь мы знаем что 3М упирается уже не в это. Можно тестировать дальше.
//Stopping metrics="granted 1285718\nhandler_time{quantile=\"0.5\"} 1.66e-07\nhandler_time{quantile=\"0.9\"} 3.34e-07\nhandler_time{quantile=\"0.97\"} 4.58e-07\nhandler_time{quantile=\"0.99\"} 5.83e-07\nhandler_time{quantile=\"1\"} 0.072861458\nhandler_time_sum 3.1324924450932095\nhandler_time_count 6000000\n"
//То есть он быстрее в 2 раза. При этом по контеншену рвет еще сильнее - 99-ая перцентиль всего 0.6 мкс =) Собственно скорее всего за счет этого и быстрее.

//Добавил ограничение на ретраи (точнее - на попытки).
//Любопытно, ранее granted 1285718 показывало, что мы не сумели выдать некоторые гранты, я думал что они просто не были выпущены;
//но теперь сомневаюсь, потому что вместо неограниченных попыток spin-lock я добавил ограничение и результат очень интересный:
//P=C S=T R=3 go run . run constant tests --rate 6000000/s  --concurrency 256
// => Stopping metrics="granted 1284507\nhandler_time{quantile=\"0.5\"} 1.66e-07\nhandler_time{quantile=\"0.9\"} 3.34e-07\nhandler_time{quantile=\"0.97\"} 4.59e-07\nhandler_time{quantile=\"0.99\"} 5.83e-07\nhandler_time{quantile=\"1\"} 0.075429208\nhandler_time_sum 3.0163244880136544\nhandler_time_count 6000000\nlost_adding 302\nlost_granting 11686\n"
// (так же потеряно 11к токенов, но видим что они потеряны в основном на ВЫДАЧЕ, где ранее вообще-то не было ограничения, возможно стоит проверить еще раз прежний вариант)
//P=C S=T R=30 go run . run constant tests --rate 6000000/s  --concurrency 256
// => Stopping metrics="granted 1294226\nhandler_time{quantile=\"0.5\"} 1.66e-07\nhandler_time{quantile=\"0.9\"} 3.75e-07\nhandler_time{quantile=\"0.97\"} 4.59e-07\nhandler_time{quantile=\"0.99\"} 6.25e-07\nhandler_time{quantile=\"1\"} 0.072911542\nhandler_time_sum 2.7223297160008206\nhandler_time_count 6000000\nlost_adding 0\nlost_granting 0\n"
// (30 ретраев достаточно чтобы обойтись без потерь на выдаче, вот тут вероятно потери были на выпуске)
//Потери на выпуске считать сложно, потому что два параллельных выпуска друг друга частично страхуют и не все токены теряются,
// так что пока непонятно за счет чего недостача (и есть ли такая же при Mutex например) - может все логично раз время теста не 1 секунда а меньше.
//Более важно, что снижение кол-ва ретраев не позволило обработать скажем 7М вместо 6М - видимо, за счет скорости конкуренция не настолько велика
// чтобы это имело значение.

//Но это были цифры для лимита 1.3М токенов в секунду. Если поднимем лимит до 3300K (половина RPS) то потери растут в абсолютном выражении:
//Lost_adding для R=3: 8400, R=2: 7670, R=1: 17700 (при R=1 стало 0.5% лимита) - это больше чем когда лимита было меньше,
// - скорее всего зависит не от RPS а именно от лимита потому что если запросов много а лимита мало - чаще выдаются 0 а они не конкурируют ни с кем
// (но не проверял)
//Lost_granting для R=3: 30K (1% лимита), R=2: 200K (5% лимита), R=1: 1M (30% лимита!) - тут доля потерь ожидаемо зависит от лимита а не RPS,
// ибо запросы сверх лимита не забирают токены - их нет - и потому не проигрывают.
//Внимание! Это были тесты для 256 "клиентов". Если поднять конкурентность до 1024 (что реалистично если в одно внешнее апи ходят 20
// наших сервисов и у каждого по 50 инстансов), получается при R=3 уже 3% от лимита. R=4 позволяет вернуться к прежним значениям,
// не проиграв при этом в RPS. R=20 снизило потери до 6 инцидентов.

//Итого мы видим что эта оптимизация стоит нам при небольших R=3-5 потерю 1% лимита и не дает ощутимого выигрыша над неограниченным спином, 
// так как при R=2 наблюдали уже всего 5% проигранных гонок от всего времени, то есть потоки мало мешают друг другу и значит это не вносит большого вклада в затраты времени.
//Но оставим ее - все равно хоть какое-то ограничение на спин нужно иметь. Раз выигрыша особого нет, а значит нет и разницы вообще,
// то можно поставить например 30. И на всякий случай следить за метриками, что не начинают возникать неожиданно высокие потери.

//А почему собственно так мало потерь на add но так много на грантовании?
//Есть примерно половина запросов которые конкурируют за извлечение из бакета но не участвуют в добавлении в бакет (посчитав что время еще не пришло)
// правда из них многие видят сразу что в бакете 0 и не конкурируют, но хватает и тех кто видят не 0
//- частично подтверждается: я поднял лимит до уровня RPS и уже видно что сооношение числа проигрышей стало из 1.8:32 => 40:180
//- но это все равно не 1:1... Пока причина неясна.

3. HA tests (with locally running etcd)

// P=C S=T R=3 go run . run constant tests --rate 6000000/s  --concurrency 256
// => Уже не справляемся. Как правило, не справляемся даже с 5500000, но это может объясняться тем, что etcd хоть и не используется
//    активно, все же ворует некоторое количество ресурсов.
// С 5400 иногда справляемся, то есть падение производительности составляет 10-15% - плата за HA

// В качестве дополнительных тестов опубликовал метрику granted через prometheus scraping endpoint,
// установил ВикториаМетрикс в качестве БД, прокинул данные в Графану, настроил частоту обновления в Графане 100мс и в ВМ тоже.
// Рассчитывал увидеть, как два инстанса вытесняют друг друга.
// Но график упорно показывал полку в последние секунды.
// В итоге выяснил, что ВикториаМетрикс задерживает данные на некоторое количество секунд (вероятно как раз на 15 по умолчанию).
// Отключил, стало лучше - но особенности этой БД такие, что в любом случае будет задержка на 1-3 секунды (примерно как в эластике,
// где рефреш индекса происходит периодически, а не постоянно) - так что в результате данные за последние 1-5 секунд сначала 
// вообще не появлялись, а затем появлялись скачком, как только становились видимы после flush.
// Пришлось вместо ВМ взять Прометеус, где этого эффекта нет.
// Запускаем /opt/homebrew/opt/prometheus/bin/prometheus_brew_services
// И видим график - после прерывания инстанса он прекращает выдавать метрики, но другой инстанс не начинает их выдавать сразу, 
// а лишь скажем через 300мс - когда у первого сгорает аренда.

// Аналогично видим что при отключении etcd инстанс дорабатывает аренду и затем перестает выдавать. Дальше через минуту
// пытается снова найти etcd, на сей раз повезло второму инстансу и он перехватил лидерство.

4. GRPC tests

In all three worker modes (even in non-blocking CAS mode) the throughput seem to cap at 50k RPS. I used the same 20 vCPU client and 20 vCPU server as with Fiber+hey, but this time with GRPC+ghz, in the same cloud. Actually, I hesitate to make conclusions that this is the limit of GRPC performance, because in Fiber tests I suspected the cloud-supplied network to be the bottleneck, and believed the real productivity to be much higher. But here the results are 2x worse than in those tests, so I cannot blame the network now. Yet 50k RPS is somewhere on lower side of results observed in different tests with lightweight payloads, I've been expecting to see maybe 100k or 150k. See local tests, though (not LAN), for futher observations on that.

Let's postpone any conclusions for now - to a moment when I conduct LAN based tests.

I however wasn't really expecting http/2 to be faster than http in general, it's just I expected good performance from the default implementation because there are claims that it is fast.

5. PProf investigation (Fiber, local machine)

Local tests with P=C R=3 are giving me up to 100K rps on my M1 8-cpu MacBook Air, using only 200% of a core.
P=W => 80k, P=M => 90K

I was curious of exactly how Fiber spends all the time, because we already established the limiter can process 5M+ RPS if http interaction is excluded from measurements.

Let's start with P=C.
As I discovered, the Go profiler in the RTL gathers blocked time and CPU time separately.
**CPU times** were like this: workerFunc (reads, parses, handles request and sends a response) = 69.3%, net.(\*conn).Read (reads request) = 29.3%, net.(\*conn).Write = 38.5%, that only leaves 1.5% inside workerFunc unaccounted for. Basically, unless I find out how to dramatically reduce costs of Read and Write, it does not make sense to look for other reasons of slowness. On the other hand 30% more samples are gathered somewhere else; actually 10% is netpoll+kevent, but 10% is 
somewhere around mPark, which seems very weird / something to investigate as TODO, because no way the application is too often left without work with 100K requests coming each second. Or can it? If requests are really processed very quickly, it could be - but then we could have been able to serve 200K instead, which we cannot. On the other hand we don't know why we cannot, because this is only a local test, maybe just it's my client is not fast enough to keep up. Another reason could be high lock contention, like, we have 100k running goroutines but all of them are waiting for something, so no other work available for the machine. If so, that would be reflected in Blocks profile, that we will look into shortly. But ultimately, that's something to look up later, but anyway even stripping away 10% of CPU time will not significantly speed us up.

Another question is, how can we see read and write on CPU sampling if those functions are supposed to be nonblocking for network sockets? I even took care to use netstat -an | grep 8000 | awk '\$2 > 0 || \$3 > 0' to see how many connections have something queued, and yes, a couple hundreds do. It seems weird, because why will data sit there and not get transferred into socket? The doorbell for transferring to network card is supposed to be signaled each microsecond or so. Does appearing on CPU samples and on netstat indicate they are blocking actually? Well no, blocking would not show them on CPU, but rather on Block profile; but they might being rather than sleeping. Also yet, if waiting happens inside a syscall, the goroutine cannot be preemted and thus will show up on CPU samples.

(Actually "list Write" says /usr/local/go/src/internal/poll/fd_unix.go calling ignoringEINTRIO(syscall.Write...) in a loop, so it's really spinning)

But no, netstat shows data in sendQ for one simple reason: they are kept there until the receiving side acks, because they might be needed for retransmitting if some chunks get lost. That means they are sitting there not for merely 1 microsecond but for entire RTT, which is of course still low, but in the range of anywhere up to 0.5 ms even on localhost, thus, given sheer number of sends, to see them on any given snapshot is not so unlikely as with 1 microsecond.

Yet, if read and write appears on CPU sampling because of spinning, why would they spin? Writing would, for large payloads, if it's are unable to put everything in a send-Q at once - but in our case we only send one response at a time in each connection and it's tiny; and reading would likely never spin. So, it's not because of spinning - then why if they are fast nonblocking calls? The answer is: nonblocking does not mean fast. If we consider the writing taking not 40% of 200% cores, but 80% of 1 core, then 100K calls per second only leaves 10 micros per call (8 micros, even), and it's not a lot of time for hardware interacting functions. Actually https://github.com/oven-sh/bun/issues/40930 measured write duration as about 4 micros; so it's in the same order of magnitude.

So I consider the mistery solved, at least until proper LAN tests.
And I consider writes and reads justified.
And I think I don't have any room for improvement here.
And I think that simply using more CPU will linearly scale thoughput, because 70% of all CPU spent by limiter is spent on reads and writes.

Oh, but that analysis would be incomplete without looking for **profile of blocks**, like I promised earlier.
93% of all time spent in block-waiting is workerFunc, that's cumulative, 82% being channel read reading from network, and 12% being serveconn (which deserializes and executes request and sends a response) - those 12% are caused by a mutex I use for collecting VictoriaMetrics, so it's not really Fiber's problem.
Is that a problem at all? Not necessarily. 1) While a goroutine is waiting for mutex, another has its chance to be executed. Unless of course they contend for same mutex! Which is to be determined 2) Total waiting time measured was just above 1 second, 10% of it is 100ms, while test duration was 22 seconds, so it's totally negligible.
then 82+12 is only 1% short of 93%, no sense in analysing that 1%. And 100-93=7% of waiting time I also don't deem worth looking into, it's even less than 10% I just discarded.

If we look at blocking implementation P=M for comparison, even though it's unlikely that read/write behavoir (which took up most of time earlier) will be different, we see that 20% of blocks is spent waiting for mutex, but it's not that bad: again, 20% of blocked time is not the same as 20% of total test time.

6. Local test

Though such tests are bound to have deficiency, they still can be useful in terms that they can produce better figures than cloud tests, especially with me only using cheaper clouds.

Running Fiber on M1 Air 8-cpu allowed me to get 100K rps with P=C (see PProf investigation above), and on M2 Pro I had been able to get 150K rps.
Actually, I had a hard time with that, because initially I only got 33K on M2 :) I have been reasonably sure that has to be because of some system settings, but there are tons of them. Actually one thing I messed up myself - downloading AMD64 hey executable instead on ARM64, which it silently executed via Rosette without any apparent warnings :) But it turned out this wasn't a reason of slowness. It was the settings, and I must admit AI proved quite valueable here, because it remembers tons of things and because of it tends not overlook little details (not always though). I gave it diff between sysctl -an on these machines, 2K lines long, and it pinpointed likely culrpits. Actually, it was wrong on priorities, but still "net.necp.pass_loopback = 2" was its finding, and although it did not explain at once the precise meaning, it of course said it has to do with doing tests on localhost. And ho, it means using installed filters for loopback packets as well as external communications. Turning that off for that M2 finally got me >100K rps.

gPRC got me 50K on M1 and 75K on M2. Same range of improvement as with Fiber, mind you, and as with CPU core count, so both are actually showing linear scaling. Interesting thing is, thought I still don't clearly see what is the bottleneck for Fiber, for gRPC it appears to be CPU consumption by ghz. Which is insane, because 75K rps I got with 8 cores used by ghz and only 2 cores used by limiter! For some unknown reason, client load generation appears to be extremely costly with gRPC, or maybe it's just ghz's problem. Anyway, extrapolating from that, one could expect limiter using all 12 cores to serve 450K rps locally with GRPC, although I'll never be able to generate that much load locally. Unless bottlenecked at some other thing, of course. Fiber at 150K consumed about 250% cores, projecting at around 720K rps using 12 cores.

Another thing worth noting is, despite profiling showing different load distribution for 1-connection and 10-connection gRPC runs, the end result remained the same, and it even got worse when I used say 50 connections. 

So we can already start making some conclusion about gRPC being allegedly faster than http 1. Its expected speed benefits come from simpler deserialization (which does not mean a lot when the payload/URL is simple) and lighter data (also does not mean a lot when payloads are little). Being able to execute multiple requests in parallel via one connection is not actually a speed benefit but rather a connection resource usage benefit, an important thing especially for outbound and not inbound limiter, but not affecting speed directly unless one runs out of available connections at the server. Besides, for limiter's model of use, in does not make sense for a caller of each request to execute new request before it received response to the last one and then executed actuall outbound request, even though the client as a whole (running different activities on multiple threads) will of course want to execute different requests in parallel.

Still, we need to wait for LAN tests before any final conclusions. Real network transfers might better react to reduced load with gRPC, for example.

7. LAN based tests

...coming as soon as I buy Thunderbolt cable

// /opt/homebrew/opt/etcd/bin/etcd

