package limiter

// NoOpProcessor grants nothing. It is used as a safe stand-in dependency when the real Processor could not be created.
type NoOpProcessor struct {
}

func (p *NoOpProcessor) Request(key string, amount int32) Response {
	return Response{Granted: 0}
}

func (p *NoOpProcessor) Close() {
	//Do nothing
}

// NoOpServer is a safe stand-in dependency when the real Server could not be created.
type NoOpServer struct {
}

func (p *NoOpServer) Shutdown() error {
	return nil
}

// Rather than make each Processor verify holding master's mantle, we decorate whatever Processor has been constucted
// with orthogonal middleware (cross-cutting concern) for verification, leaving Processor and consensus-tracker uncoupled.
type GuardedProcessor struct {
	next    Processor
	tracker ConsensusTracker
}

func NewGuardedProcessor(next Processor, tracker ConsensusTracker) *GuardedProcessor {
	return &GuardedProcessor{next: next, tracker: tracker}
}

func (p *GuardedProcessor) Request(key string, amount int32) Response {
	if !p.tracker.IAmMaster() {
		//TODO think of some way of signaling that the client is using the wrong instance of server and it's not normal 429
		// maybe some HTTP response codes are already well suited for that
		return Response{Granted: 0}
	}
	return p.next.Request(key, amount)
}

func (p *GuardedProcessor) Close() {
	p.next.Close()
}
