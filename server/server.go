package server

import (
	"time"
	etcd "go.etcd.io/etcd/client/v3"
)

type Response struct {
	Granted int32
}

// In worker-pool approach, worker goroutines never complete, but they are reused and that saves some allocations etc.
// In semaphore-protected approach, answering routines are completed and cleanly recreated, that's cleaner
//	(no need to guarantee goroutine will not die out of panic) but more expensive.
// Also, since I don't want more than 1 request processed at each moment, I can use mutex-protected approach.
//	It does not use allocations and does not need goroutine creation at all, and appears like most efficient solution.
// What do I pick? I'll allow selecting between worker-pool and mutex, because I have some reservation about mutex drawbacks,
//  and need to do measurements. On second thought I also decided to introduce lock-free algorithm as third option.
type Processor interface {
	// This method is intrinsically safe for concurrent use by 
	// multiple goroutines. However, the Processor itself in general case does not 
	// manage configuration safety; the caller must ensure the Processor 
	// is safely published before concurrent execution begins.
	Request(key string, amount int32) Response

	//Relies on no concurrent/subsequent calls to request()
	//(if that is idiomatic expectation then we don't need that comment)
	Close() //close is the idiomatic name for stopping lifecycle and releasing resources
}

type Server interface {
	Shutdown() error
}

type Config struct {
	Port       int              `yaml:"port"`
	MaxRequest int32            `yaml:"max_requests"`
	APIs       map[string]int32 `yaml:"api"`
	ETCD	   etcd.Config     `yaml:"etcd"`
	ETCDkey	   string			`yaml:"etcd-key"`
}

type Bucket struct {
	Count     int32
	Limit     int32
	StartedAt time.Time
	Issued    int64
}

type ConsensusTracker interface {
	IAmMaster() bool
}

// TODO: how critical can possible time leap be? Like, in "leap second" or "switch to daylight time"
func Refill(bucket *Bucket, now time.Time) {
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
	if bucket.Count > limit {
		bucket.Count = limit
	}
}

//TODO: config for acceptance limit Q (max sum quota) and acceptance multiplier K; load shedding over capacity = L * K
//      (google's default is K=2.0, provided that rejection path is much shorter than acceptance path, but I'll need to measure
//       whether that's my case or not, and maybe set it to some LOWER value - after all, by serving over Q, we only want
//.      to propagate feedback to clients, we don't necessarily need to serve those extra requests out of pure principle;
//.      thus, if that's too costly, we are going to just ignore them).*
//.      * Setting the capacity closer to Q, however, means load-shedding is more likely to happen, and it can hurt well-behaving
//.        clients; that's bad, but if it's good enough compromise for Google, I'll take it.
//         Thus, picking good value for K must be tuned for each specific system, minding how compliant the clients are expected to be.

// Итак мы должны создать где-то клиента
// В конце отпустить
// Ну а пока он нужен - какая схема?

// Мы как обычно делаем то что делаем, а в начале всего этого берем аренду. Ну то есть если взяли, то делаем что обычно делаем.
// И пытаемся продлевать аренду в фоне.
// Если очередное продление не получается - запускаем шатдаун.

// Или так
// Мы как обычно делаем то что делаем
// Вначале мы не мастер
// Когда приходит запрос - мы смотрим, и если видим что мы не мастер - сразу отлуп (429 или 500 или код для not ready) - желательно
// такой код чтобы получатель понял что надо выбрать другой инстанс
// Затруднение тут это дополнительный флаг который надо синхронизированно читать

// В фоне цикл который пытается захватить лидерство
// если получается - он взводит флаг и дальше пытается продлевать аренду. Если очередное продление не прокатывает - мы превентивно сразу сбрасываем 
// флаг, чтобы у нас осталось еще скажем 0.2 сек на то чтобы доработали текущие уже запущенные запросы, но новые уже не будем удовлетворять
// и тогда снова возвращаемся к этапу "попытаться захватить лидерство"
// при этом ломиться в лидеры постоянно смысла нет, мы должны прочитать когда заканчивается текущая аренда и подождать до этого момента и лишь
// потом пробовать. В этот период же не нужен хартбит? (или его вообще делают прозрачно для нас?)