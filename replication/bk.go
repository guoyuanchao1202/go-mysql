package replication
//
//import (
//	"context"
//	"sync"
//)
//
//const (
//	DefaultConcurrency                 = 8
//	DefaultQueueSize                   = 1024
//	DefaultBatchSize                   = 26
//	DefaultBufferedChanSize = 17
//	InitSequence                       = uint64(1)
//	InitWorkerNo                       = 0
//)
//
//type workerInput struct {
//	sequence          uint64 // batch sequence
//	data              []byte // binlog event
//	enableSemiSyncAck bool   // need semiSync to mysql master
//	initParser        bool   // if FormatDescriptionEvent, enable this
//	needSend          bool   // if false, drop event after parse
//}
//
//
//func (input *workerInput) setSequence(sequence uint64) *workerInput {
//	input.sequence = sequence
//	return input
//}
//
//func (input *workerInput) setData(data []byte) *workerInput {
//	input.data = data
//	return input
//}
//
//func (input *workerInput) setEnableSemiSyncAck(ack bool) *workerInput {
//	input.enableSemiSyncAck = ack
//	return input
//}
//
//func (input *workerInput) setSendFlag(sendFlag bool) *workerInput {
//	input.needSend = sendFlag
//	return input
//}
//
//func (input *workerInput) setInitParser(initParser bool) *workerInput {
//	input.initParser = initParser
//	return input
//}
//
//func (input *workerInput) reset() {
//	input.sequence = 0
//	input.needSend = false
//	input.initParser = false
//	input.data = nil
//	input.enableSemiSyncAck = false
//}
//
//type asyncEventProcessor struct {
//	ctx    context.Context // exit
//	cancel context.CancelFunc
//	close  bool
//	wg     sync.WaitGroup
//
//	concurrency  int // concurrency
//	workerQueues []chan *workerInput
//	queueSize    int
//
//	sequence            uint64 // keep event order
//	nextSendingSequence uint64
//
//	MaxBatchSize    int // batch
//	currentBatchNum int
//
//	currentWorker       chan *workerInput // runtime info
//	nextWorkerIndex     int
//	prevIsTableMapEvent bool
//
//	updatePosAndGTIDCallback func(e *BinlogEvent) error // callback
//	replySemiSyncCallback    func() error
//
//	parserTemplate *BinlogParser // parser
//
//	*workerInputPool // workerInput pool
//}
//
//type workerInputPool struct {
//	pool                  *sync.Pool // workInput pool
//	workerInputBufferChan chan *workerInput
//}
//
//
////type goroutinePool struct {
////	ctx      context.Context
////	cancel   context.CancelFunc
////	size     int
////	argInput chan *workerInput
////	idleNum  int32
////}
////
////func (gp *goroutinePool) start() {
////	for i := 0; i < gp.size; i++ {
////		go func() {
////			select {
////			case <-gp.ctx.Done():
////				return
////			case input := <-gp.argInput:
////				atomic.AddInt32(&gp.idleNum, -1)
////
////				atomic.AddInt32(&gp.idleNum, 1)
////			}
////		}()
////	}
////}
////
////func (gp *goroutinePool) submit(input *workerInput) {
////	gp.argInput <- input
////}
//
//func newAsyncEventProcessor(concurrency int, queueSize int, parser *BinlogParser) *asyncEventProcessor {
//	p := &asyncEventProcessor{
//		wg:                  sync.WaitGroup{},
//		concurrency:         DefaultConcurrency,
//		workerQueues:        make([]chan *workerInput, DefaultConcurrency),
//		queueSize:           DefaultQueueSize,
//		sequence:            InitSequence,
//		nextSendingSequence: InitSequence,
//		MaxBatchSize:        DefaultBatchSize,
//		nextWorkerIndex:     InitWorkerNo + 1,
//		parserTemplate:      parser,
//	}
//
//	p.workerInputPool = &workerInputPool{
//		pool: &sync.Pool{
//			New: func() interface{} {
//				return &workerInput{}
//			},
//		},
//		workerInputBufferChan: make(chan *workerInput, DefaultBufferedChanSize),
//	}
//
//	p.ctx, p.cancel = context.WithCancel(context.Background())
//	return p
//}
//
//func (p *asyncEventProcessor) getWorkerInput() (input *workerInput) {
//	select {
//	case input = <-p.workerInputBufferChan:
//	default:
//		input = p.pool.Get().(*workerInput)
//	}
//	input.reset()
//	return
//}
//
//func (p *asyncEventProcessor) putWorkerInput(input *workerInput) {
//	if input == nil {
//		return
//	}
//	select {
//	case p.workerInputBufferChan <- input:
//	default:
//		p.pool.Put(input)
//	}
//}
//
//func (p *asyncEventProcessor) Start() {
//	for i := 0; i < p.concurrency; i++ {
//
//		p.wg.Add(1)
//		p.workerQueues[i] = make(chan *workerInput, p.queueSize)
//
//		go p.worker(i, p.workerQueues[i])
//
//	}
//	p.currentWorker = p.workerQueues[InitWorkerNo]
//}
//
//func (p *asyncEventProcessor) Close() {
//	p.close = true
//	p.cancel()
//	p.wg.Wait()
//}
//
//func (p *asyncEventProcessor) worker(workerID int, queue chan *workerInput) {
//	defer func() {
//		p.wg.Done()
//	}()
//
//	var format *FormatDescriptionEvent
//	// pending := make([][]*workerInput, 0)
//	parser := p.parserTemplate.Clone()
//	// ticker := time.NewTicker(20 * time.Millisecond)
//
//	for input := range queue {
//		if input.initParser {
//			event, err := parser.Parse(input.data)
//			if err != nil {
//				return
//			}
//			format = event.Event.(*FormatDescriptionEvent)
//			parser.format = format
//		}
//
//
//
//	}
//
//	for {
//		select {
//		case <-p.ctx.Done():
//			return
//		case input := <-queue:
//
//			if input.initParser {
//				event, err := parser.Parse(input.data)
//				if err != nil {
//					return
//				}
//				format = event.Event.(*FormatDescriptionEvent)
//				parser.format = format
//			}
//
//			//if input.sequence != 0 {
//			//	pending = append(pending, make([]*workerInput, 0, DefaultBatchSize+3))
//			//}
//			//
//			//fmt.Println(input.sequence, len(pending), len(pending) - 1, len(pending[0]))
//			//
//			//pending[len(pending)-1] = append(pending[len(pending)-1], input)
//
//			//case <-ticker.C:
//			//	if len(pending) < 2{
//			//		ticker.Reset(20 * time.Millisecond)
//			//		continue
//			//	}
//			//
//			//	for _, input := range pending[0] {
//			//		p.putWorkerInput(input)
//			//	}
//			//
//			//	pending = pending[1:]
//			//	ticker.Reset(20 * time.Millisecond)
//		}
//	}
//
//}
//
//
//func (p *asyncEventProcessor) SetUpdatePosAndFTIDCallback(callback func(e *BinlogEvent) error) {
//	p.updatePosAndGTIDCallback = callback
//}
//
//func (p *asyncEventProcessor) SetReplySemiSyncCallback(callback func() error) {
//	p.replySemiSyncCallback = callback
//}
//
//func (p *asyncEventProcessor) Process(eventType EventType, data []byte, enableSemiSyncAck bool) {
//	switch eventType {
//	case TABLE_MAP_EVENT:
//		p.processTableMapEvent(data, enableSemiSyncAck)
//		return
//	case FORMAT_DESCRIPTION_EVENT:
//		p.processFormatDescriptionEvent(data, enableSemiSyncAck)
//	case WRITE_ROWS_EVENTv0,
//		UPDATE_ROWS_EVENTv0,
//		DELETE_ROWS_EVENTv0,
//		WRITE_ROWS_EVENTv1,
//		DELETE_ROWS_EVENTv1,
//		UPDATE_ROWS_EVENTv1,
//		WRITE_ROWS_EVENTv2,
//		UPDATE_ROWS_EVENTv2,
//		DELETE_ROWS_EVENTv2:
//		p.processRowsEvent(data, enableSemiSyncAck)
//	default:
//		p.processNormalEvent(data, enableSemiSyncAck)
//	}
//	p.prevIsTableMapEvent = false
//}
//
//func (p *asyncEventProcessor) processFormatDescriptionEvent(data []byte, enableSemiSyncAck bool) {
//
//	p.currentWorker <- p.getWorkerInput().setData(data).setEnableSemiSyncAck(enableSemiSyncAck).
//		setSendFlag(true).setInitParser(true)
//
//	for _, worker := range p.workerQueues {
//
//		if worker != p.currentWorker {
//			worker <- p.getWorkerInput().setData(data).setInitParser(true)
//		}
//
//	}
//}
//
//func (p *asyncEventProcessor) processTableMapEvent(data []byte, enableSemiSyncAck bool) {
//
//	if p.prevIsTableMapEvent {
//		p.processRowsEvent(data, enableSemiSyncAck)
//		return
//	}
//
//	p.prevIsTableMapEvent = true
//
//	if p.currentBatchNum >= p.MaxBatchSize-1 {
//		p.changeWorker()
//	}
//
//	p.processNormalEvent(data, enableSemiSyncAck)
//}
//
//func (p *asyncEventProcessor) processRowsEvent(data []byte, enableSemiSyncAck bool) {
//	p.currentWorker <- p.getWorkerInput().setData(data).setEnableSemiSyncAck(enableSemiSyncAck)
//}
//
//func (p *asyncEventProcessor) processNormalEvent(data []byte, enableSemiSyncAck bool) {
//	input := p.getWorkerInput().setData(data).setEnableSemiSyncAck(enableSemiSyncAck)
//
//	// if first event in batch, set sequence
//	if p.currentBatchNum == 0 {
//		input.setSequence(p.sequence)
//	}
//
//	// send event to current worker
//	p.currentBatchNum++
//	p.currentWorker <- input
//
//	// if current batch is end, update sequence and current worker
//	if p.currentBatchNum > p.MaxBatchSize {
//		p.changeWorker()
//	}
//}
//
//func (p *asyncEventProcessor) changeWorker() {
//	p.sequence++
//	p.currentBatchNum = 0
//	p.currentWorker = p.workerQueues[p.nextWorkerIndex]
//	p.nextWorkerIndex = (p.nextWorkerIndex + 1) % p.concurrency
//}
