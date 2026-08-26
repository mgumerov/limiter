package main

import (
	"log"
	"context"
	
	"os/signal"
	"os"
	"syscall"
	"sync"

	"github.com/gofiber/fiber/v3"
)

type Request struct {
	amount int
	reply chan<- Response
}

type Response struct {
	granted int
}

type Processor struct {
	rqChan chan<- Request
}

func main() {
	//тут нам по api key по идее нужно найти бакет
	//а вообще скорее найти канал в который положить операцию,
	//ведь запросы выполняются последовательно.
	
	// Будет вообще-то
	// проще если на все будет один канал (вопросы деления на ключи
	// и потенциально шардирования не будем пока рассматривать)

	// Этот один канал либо должен быть с большим буфером
	// (но даже так функция тут не сможет вернуть управление клиенту
	// не дождавшись вердикта)
	// Какие есть паттерны того чтобы вернуть клиенту ответ асинхронно?
	// Ну а если канал не имеет буфера, что будет, блокировка? Ну условно конечно, на самом-то деления
	// горутину просто отложат в сторону.

	// Ну и по хорошему этот канал должен жить в отдельном "сервисе", в смысле что туда будут
	// кидать обе мои реализации "сервера" - fiber и grpc

	// If you need to guarantee that the receiver actually got the data before the sender moves on, use an unbuffered channel.
	// - у меня и так и не так, я могу просто закончить но тогда потом на then надо умудриться послать ответ
	// - что на это соображение скажет ИИ?

	// горутины-воркеры никогда не заканчиваются но зато реюзаются
	// горутины в семафорной модели (горутины под задачу) заканчиваются и это надежнее (чистый рестарт) - зато не реюзаются
	// Впрочем "не заканчиваются" для простых рутин не очень плохо, защита от паники как исключений
	// прицепляется легко, а длительных по времени операций (или обращений куда-то) в моем последнем
	// варианте вроде бы нет, зависать нечему. А если фатальная ошибка то упадет не одна рутина а все приложение.

	var processor *Processor = startProcessor()

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

	//In general case, with many subsystems like Fiber, we don't know how many errors we might face, 
	// so we only process only (and we only need 1, because these are critical erros and we halt on them), 
	// and we use sync.Once to make sure no one is trying to actually send more (and panic because no one listens anymore)
	fatal := make(chan string, 1)
	var once sync.Once

	//Idiomatic approach says passing contexts along (here, or/and to fiber.Listen) because this clearly involves some 
	// i/o and long activity. But actually it depends on how the called func will use the context: maybe I am passing 
	// request-scoped context to some async processing, for example. In this case, passing globally-scoped notify-context 
	// actually makes sense, but only if I want Fiber to use it for graceful shutdowns, and I stated above that I prefer it not to.
	fiber := createHTTP(processor)
	startHTTP(fiber, func(err string) {
		//since it's closure and not explicit passing of "once", the language guarantees it's passed by reference,
		// which is critical (we want to use same instance of this "once" everywhere)
		once.Do(func() { fatal <- err } )
	})

	select {
	case err := <- fatal:
		log.Fatal(err);
	case <- ctx.Done(): 
		if err := fiber.Shutdown(); err != nil {
			log.Print(err)
		}
		processor.close()
	}
}

func startProcessor() *Processor {
	reqChan := make(chan Request) //unbuffered, because what good it is under heavy load?
	
	//Process all requests sequentially by a single goroutine
	go func() {
		limit := 2

		//TODO handle panics
		//Note, range loop, just like 2-argument reading form, checks for closing the channel,
		// we'll use it to propagate graceful shutdown to our goroutine
		for request := range reqChan {
			if limit < request.amount {
				request.reply <- Response { granted: 0 }
			} else {
				limit -= request.amount
				request.reply <- Response { granted: request.amount }
			}
		}
	}()

	return &Processor { rqChan: reqChan }
}

//Although idiomatic (responding via channel), implementation is still better be hidden
func (p *Processor) request(amount int) Response {
	//buffered, because it makes no sense to block when responding, however little are chances;
	// also, this avoids depending on the receiving side _still being alive_ (might have panicked or whatever)
	//TODO potentially a performace hindrance, say 1M allocations/second, need to somehow measure and try another approach
	reply := make(chan Response, 1)
	p.rqChan <- Request { amount: amount, reply: reply }
	return <- reply
}

//Relies on no concurrent/subsequent calls to request()
//(if that is idiomatic expectation then we don't need that comment)
func (p *Processor) close() { //close is the idiomatic name for stopping lifecycle and releasing resources
	close(p.rqChan)
}

func createHTTP(processor *Processor) *fiber.App {
	app := fiber.New()

	app.Get("/", func(c fiber.Ctx) error {
		if processor.request(1).granted == 1 {
			return c.SendString("c.SendStatus(fiber.StatusOK)")
		} else {
			return c.SendString("c.SendStatus(fiber.StatusTooManyRequests)")
		}
	})

	return app
}

func startHTTP(fiber *fiber.App, onError func(err string)) {
	go func() {
		//startup errors return non-nil, graceful shutdown returns nil, shutdown errors are only returned via shutdown()
		if err := fiber.Listen(":3000"); err != nil { 
			onError("HTTP Server failed")
			log.Print(err)
		}
	}()
}
