package main

import (
	"log"

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
	app := fiber.New()

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

	app.Get("/", func(c fiber.Ctx) error {
		if processor.request(1).granted == 1 {
			return c.SendString("c.SendStatus(fiber.StatusOK)")
		} else {
			return c.SendString("c.SendStatus(fiber.StatusTooManyRequests)")
		}
	})

	log.Fatal(app.Listen(":3000"))
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
	reply := make(chan Response, 1)
	p.rqChan <- Request { amount: amount, reply: reply }
	return <- reply
}

//TODO support actually calling this :) To implement graceful shutdown
func (p *Processor) close() { //close is the idiomatic name for stopping lifecycle and releasing resources
	close(p.rqChan)
}
