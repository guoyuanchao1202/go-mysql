package replication

import (
	"context"
	"sync"
)

const (
	DefaultConcurrency      = 8
	DefaultQueueSize        = 4096
	DefaultBufferedChanSize = 17
	InitSequence            = uint64(1)
	InitWorkerNo            = 0
)

type workerInput struct {
	sequence   uint64 // batch sequence
	initParser bool
	events     []*eventInfo
}

func (input *workerInput) setSequence(sequence uint64) *workerInput {
	input.sequence = sequence
	return input
}

func (input *workerInput) addEvent(event *eventInfo) *workerInput {
	if input.events == nil {
		input.events = make([]*eventInfo, 0, 10)
	}
	input.events = append(input.events, event)
	return input
}

func (input *workerInput) setInitParser(initParser bool) *workerInput {
	input.initParser = initParser
	return input
}

func (input *workerInput) reset() {
	input.sequence = 0
	input.initParser = false
	input.events = input.events[:0]
}

type eventInfo struct {
	data              []byte // binlog event
	enableSemiSyncAck bool   // need semiSync to mysql master
}

func (e *eventInfo) setData(data []byte) *eventInfo {
	e.data = data
	return e
}

func (e *eventInfo) setEnableSemiSync(ack bool) *eventInfo {
	e.enableSemiSyncAck = ack
	return e
}

func (e *eventInfo) reset() *eventInfo {
	e.data = nil
	e.enableSemiSyncAck = false
	return e
}

type asyncEventProcessor struct {
	ctx    context.Context // exit
	cancel context.CancelFunc
	close  bool
	wg     sync.WaitGroup

	concurrency  int // concurrency
	workerQueues []chan *workerInput
	queueSize    int

	sequence            uint64 // keep event order
	nextSendingSequence uint64

	currentWorkerInput *workerInput // runtime info
	currentWorker      chan *workerInput
	nextWorkerIndex    int

	updatePosAndGTIDCallback func(e *BinlogEvent) error // callback
	replySemiSyncCallback    func() error

	parserTemplate *BinlogParser // parser

	*workerInputPool // workerInput pool
}

type workerInputPool struct {
	inputPool             *sync.Pool // workInput pool
	workerInputBufferChan chan *workerInput

	eventPool       *sync.Pool
	eventBufferChan chan *eventInfo
}

//type goroutinePool struct {
//	ctx      context.Context
//	cancel   context.CancelFunc
//	size     int
//	argInput chan *workerInput
//	idleNum  int32
//}
//
//func (gp *goroutinePool) start() {
//	for i := 0; i < gp.size; i++ {
//		go func() {
//			select {
//			case <-gp.ctx.Done():
//				return
//			case input := <-gp.argInput:
//				atomic.AddInt32(&gp.idleNum, -1)
//
//				atomic.AddInt32(&gp.idleNum, 1)
//			}
//		}()
//	}
//}
//
//func (gp *goroutinePool) submit(input *workerInput) {
//	gp.argInput <- input
//}

func newAsyncEventProcessor(concurrency int, queueSize int, parser *BinlogParser) *asyncEventProcessor {
	p := &asyncEventProcessor{
		wg:                  sync.WaitGroup{},
		concurrency:         DefaultConcurrency,
		workerQueues:        make([]chan *workerInput, DefaultConcurrency),
		queueSize:           DefaultQueueSize,
		sequence:            InitSequence,
		nextSendingSequence: InitSequence,
		nextWorkerIndex:     InitWorkerNo + 1,
		parserTemplate:      parser,
	}

	p.workerInputPool = &workerInputPool{
		inputPool: &sync.Pool{
			New: func() interface{} {
				return &workerInput{}
			},
		},
		workerInputBufferChan: make(chan *workerInput, DefaultBufferedChanSize),

		eventPool: &sync.Pool{
			New: func() interface{} {
				return &eventInfo{}
			},
		},
		eventBufferChan: make(chan *eventInfo, DefaultBufferedChanSize),
	}

	p.currentWorkerInput = p.getWorkerInput().setSequence(InitSequence)

	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p
}

// getWorkerInput get workerInput instance from pool
func (p *asyncEventProcessor) getWorkerInput() (input *workerInput) {
	select {
	case input = <-p.workerInputBufferChan:
	default:
		input = p.inputPool.Get().(*workerInput)
	}

	input.reset()
	return
}

// putWorkerInput put workerInput instance to pool
func (p *asyncEventProcessor) putWorkerInput(input *workerInput) {
	if input == nil {
		return
	}

	// put event to pool
	for _, event := range input.events {
		p.putEventInfo(event)
	}

	select {
	case p.workerInputBufferChan <- input:
	default:
		p.inputPool.Put(input)
	}
}

func (p *asyncEventProcessor) getEventInfo() (event *eventInfo) {
	select {
	case event = <-p.eventBufferChan:
	default:
		event = p.eventPool.Get().(*eventInfo)
	}

	event.reset()
	return
}

func (p *asyncEventProcessor) putEventInfo(event *eventInfo) {
	if event == nil {
		return
	}

	select {
	case p.eventBufferChan <- event:
	default:
		p.eventPool.Put(event)
	}
}

func (p *asyncEventProcessor) Start() {

	for i := 0; i < p.concurrency; i++ {

		p.wg.Add(1)
		p.workerQueues[i] = make(chan *workerInput, p.queueSize)

		go p.worker()

	}

	p.currentWorker = p.workerQueues[InitWorkerNo]

}

func (p *asyncEventProcessor) Close() {
	p.close = true
	p.cancel()
	p.wg.Wait()
}

func (p *asyncEventProcessor) worker() {
	defer func() {
		p.wg.Done()
	}()

	var format *FormatDescriptionEvent
	pending := make([]*workerInput, 0)
	parser := p.parserTemplate.Clone()

	for input := range p.currentWorker {
		if p.close {
			return
		}

		// if initParser, just parse and init, then drop it
		if input.initParser {
			event, err := parser.Parse(input.events[0].data)
			if err != nil {
				return
			}
			format = event.Event.(*FormatDescriptionEvent)
			parser.format = format
		}

		pending = append(pending, input)

		select {

		default:

		}

		p.putWorkerInput(input)
	}
}

func (p *asyncEventProcessor) SetUpdatePosAndFTIDCallback(callback func(e *BinlogEvent) error) {
	p.updatePosAndGTIDCallback = callback
}

func (p *asyncEventProcessor) SetReplySemiSyncCallback(callback func() error) {
	p.replySemiSyncCallback = callback
}

// Process receive event binlog
func (p *asyncEventProcessor) Process(eventType EventType, data []byte, enableSemiSyncAck bool) {
	if eventType == FORMAT_DESCRIPTION_EVENT {
		p.processFormatDescriptionEvent(data, enableSemiSyncAck)
		return
	}

	if eventType == XID_EVENT {
		p.processXIDEvent(data, enableSemiSyncAck)
		return
	}

	p.processNormalEvent(data, enableSemiSyncAck)
}

// processFormatDescriptionEvent need to send this event to all worker to init parser
func (p *asyncEventProcessor) processFormatDescriptionEvent(data []byte, enableSemiSyncAck bool) {

	p.currentWorkerInput.addEvent(p.getEventInfo().setData(data).setEnableSemiSync(enableSemiSyncAck))

	for _, worker := range p.workerQueues {
		worker <- p.getWorkerInput().addEvent(p.getEventInfo().setData(data)).setInitParser(true)
	}
}

// processNormalEvent just add to current transaction buffer
func (p *asyncEventProcessor) processNormalEvent(data []byte, enableSemiSyncAck bool) {

	p.currentWorkerInput.addEvent(p.getEventInfo().setData(data).setEnableSemiSync(enableSemiSyncAck))
}

// processXIDEvent send current transaction to current worker and change worker
func (p *asyncEventProcessor) processXIDEvent(data []byte, enableSemiSyncAck bool) {

	p.currentWorkerInput.addEvent(p.getEventInfo().setData(data).setEnableSemiSync(enableSemiSyncAck))

	p.currentWorker <- p.currentWorkerInput

	p.changeWorker()
}

func (p *asyncEventProcessor) changeWorker() {
	p.sequence++
	p.currentWorker = p.workerQueues[p.nextWorkerIndex]
	p.nextWorkerIndex = (p.nextWorkerIndex + 1) % p.concurrency
	p.currentWorkerInput = p.getWorkerInput().setSequence(p.sequence)
}
