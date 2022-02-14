package replication

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
)

const (
	DefaultConcurrency = 8
	DefaultBufferSize  = 32
	MaxConcurrency     = 2048
)

// workerInput asyncProcessor's worker input
type workerInput struct {
	sequence          uint64
	data              []byte
	needAckCoordinate bool
	needSemiSync      bool
}

// ackInput asyncProcessor's coordinator input
type ackInput struct {
	sequence     uint64
	needAckMySQL bool
	event        *BinlogEvent
}

type asyncEventProcessor struct {
	ctx      context.Context // used close
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	isClosed int32
	mu       sync.Mutex

	reset     chan struct{} // used reset
	resetWait chan struct{}

	concurrency       int // used init worker
	processQueues     []chan *workerInput
	savedWorkerQueue  chan *workerInput
	processBufferSize int

	workerSelector uint64 // used to select which worker process event

	processSequence uint64 // used to ack to coordinator
	toAckSeqInput   chan uint64
	ackSeqInput     chan *ackInput

	parserTemplate *BinlogParser   // used to generate parser for every worker
	streamer       *BinlogStreamer // used to send event, mainly for compatibility

	updatePosAndGTIDCallback func(e *BinlogEvent) error // callback
	replySemiSyncCallback    func() error
}

func newAsyncEventProcessor(concurrency, bufferSize int, parser *BinlogParser, streamer *BinlogStreamer) *asyncEventProcessor {
	if concurrency <= 0 || concurrency > MaxConcurrency {
		concurrency = DefaultConcurrency
	}

	concurrency = adjustToPowOfTwo(concurrency)

	if bufferSize < 0 {
		bufferSize = DefaultBufferSize
	}

	res := &asyncEventProcessor{
		wg:                sync.WaitGroup{},
		mu:                sync.Mutex{},
		reset:             make(chan struct{}),
		resetWait:         make(chan struct{}, concurrency),
		concurrency:       concurrency,
		processQueues:     make([]chan *workerInput, 0, concurrency),
		processBufferSize: bufferSize,
		workerSelector:    uint64(concurrency - 1),
		toAckSeqInput:     make(chan uint64),
		ackSeqInput:       make(chan *ackInput, concurrency*20),
		streamer:          streamer,
		parserTemplate:    parser,
	}

	res.ctx, res.cancel = context.WithCancel(context.Background())

	return res
}

func (p *asyncEventProcessor) Start() {
	for i := 0; i < p.concurrency; i++ {

		p.wg.Add(1)
		p.processQueues = append(p.processQueues, make(chan *workerInput, p.processBufferSize))
		go p.worker(p.processQueues[i])

	}

	p.wg.Add(1)

	go p.coordinate()
}

func (p *asyncEventProcessor) Stop() {
	p.stopWithError(nil)
}

// Reset will reset every worker's parser.format, this operation needs to wait until the worker has no pending events
func (p *asyncEventProcessor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.reset)
	p.reset = make(chan struct{})

	respCount := 0
	for range p.resetWait {
		respCount++

		if respCount == p.concurrency {
			return
		}
	}
}

// Process used to process binlog data
// params:
// data: 				binary of binlog event
// needChangeProcessor: if true then use different worker to process this event; or while use saved worker to process event
// save: 				if true then save worker that process current event
// sendAll: 			if true then send event to all worker
// needSemiSync:  		mysql semiSync use
func (p *asyncEventProcessor) Process(data []byte, needChangeProcessor, save, sendAll, needSemiSync bool) {
	// add toAck event to coordinator
	seq := p.processSequence
	p.addToAck(seq)
	p.incSequence()

	// send to all worker, only one worker need ack to coordinator
	if sendAll {
		p.sendMessageToAllWorker(data, seq, needSemiSync)
		return
	}

	// use before event same worker
	ch := p.savedWorkerQueue
	if needChangeProcessor || ch == nil {
		ch = p.processQueues[int(seq&p.workerSelector)]
	}

	// save this event worker
	if save {
		p.savedWorkerQueue = ch
	}

	// send to worker
	ch <- &workerInput{
		sequence:          seq,
		data:              data,
		needAckCoordinate: true,
		needSemiSync:      needSemiSync,
	}

}

func (p *asyncEventProcessor) stopWithError(err error) {
	p.streamer.closeWithError(err)

	// only call once
	if atomic.CompareAndSwapInt32(&p.isClosed, 0, 1) {
		p.cancel()
		p.wg.Wait()
	}
}

func (p *asyncEventProcessor) SetUpdatePosAndFTIDCallback(callback func(e *BinlogEvent) error) {
	p.updatePosAndGTIDCallback = callback
}

func (p *asyncEventProcessor) SetReplySemiSyncCallback(callback func() error) {
	p.replySemiSyncCallback = callback
}


func (p *asyncEventProcessor) worker(ch chan *workerInput) {
	defer p.wg.Done()

	parser := p.parserTemplate.Clone()
	needReset := false
	for {
		select {
		case <-p.ctx.Done(): // close
			return
		case <-p.reset: // reset parser
			needReset = true
		case input := <-ch:

			// parse event
			event, err := parser.Parse(input.data)
			if err != nil {
				p.stopWithError(err)
				return
			}

			// no need ack to coordinator, continue
			if !input.needAckCoordinate {
				continue
			}

			// ack to coordinator
			p.ackEvent(&ackInput{
				sequence:     input.sequence,
				needAckMySQL: input.needSemiSync,
				event:        event,
			})

			// need reset
			if needReset && len(ch) == 0 {
				parser.format = nil
				needReset = false
				p.resetWait <- struct{}{}
			}
		}
	}
}


func (p *asyncEventProcessor) sendMessageToAllWorker(data []byte, sequence uint64, needSemiSync bool) {
	firstMessage := true

	// send data to all worker, but only one worker need ack coordinator
	for _, processChan := range p.processQueues {
		processChan <- &workerInput{
			sequence:          sequence,
			data:              data,
			needAckCoordinate: firstMessage,
			needSemiSync:      needSemiSync,
		}

		if firstMessage {
			firstMessage = false
		}
	}
}

func (p *asyncEventProcessor) incSequence() {
	if p.processSequence == math.MaxUint64 {
		p.processSequence = 0
	}
	p.processSequence += 1
}

func (p *asyncEventProcessor) addToAck(seq uint64) {
	p.toAckSeqInput <- seq
}

func (p *asyncEventProcessor) ackEvent(ackInput *ackInput) {
	p.ackSeqInput <- ackInput
}

// coordinate The worker parses the event concurrently, and then the coordinator ensures that
// the order of the sent events is the same as the order of the received events, and does the subsequent processing,
// like semiSync、update pos and gtid.
func (p *asyncEventProcessor) coordinate() {
	defer p.wg.Done()

	toAckSequenceQueue := make([]uint64, 0)
	ackedSequenceSet := make(map[uint64]*ackInput)

	for {
		select {
		case <-p.ctx.Done():
			return

		case seq := <-p.toAckSeqInput:
			toAckSequenceQueue = append(toAckSequenceQueue, seq)

		case input := <-p.ackSeqInput:
			ackedSequenceSet[input.sequence] = input

			increaseSteps := 0
			for _, toAckSeq := range toAckSequenceQueue {
				ack, ok := ackedSequenceSet[toAckSeq]
				if !ok {
					break
				}
				increaseSteps++
				delete(ackedSequenceSet, toAckSeq)

				// update pos and gtid callback if callback isn't nil
				if p.updatePosAndGTIDCallback != nil {
					if err := p.updatePosAndGTIDCallback(ack.event); err != nil {
						p.stopWithError(err)
						return
					}
				}

				// send message out
				p.streamer.ch <- ack.event

				// if no need ack mysql or no ackFunc, continue
				if !ack.needAckMySQL || p.replySemiSyncCallback == nil {
					continue
				}

				// ack mysql master callback
				if err := p.replySemiSyncCallback(); err != nil {
					p.stopWithError(err)
					return
				}
			}

			// remove sent event from toAck queue
			toAckSequenceQueue = toAckSequenceQueue[increaseSteps:]
		}
	}
}

// adjustToPowOfTwo adjust num to mix powOfTwo that bigger then num
func adjustToPowOfTwo(num int) int {
	bitMover := 1 << 62
	if num > bitMover || (num < 0 && num <= -math.MaxInt64) {
		return bitMover
	}
	if num < 0 {
		num = -num
	}

	firstMark := 0
	alreadyFound := false
	for bitMover != 0 {

		if (bitMover & num) != bitMover {
			bitMover >>= 1
			continue
		}

		if alreadyFound {
			return firstMark << 1
		}
		alreadyFound = true
		firstMark = bitMover

	}
	return firstMark
}
