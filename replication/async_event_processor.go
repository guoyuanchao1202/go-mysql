package replication

import (
	"context"
	"sync"
	"time"
)

// todo: 测试不使用任何channel
// todo: 测试不使用批量
// todo: 优化parse函数

const (
	DefaultConcurrency      = 16
	DefaultQueueSize        = 2048
	DefaultBufferedChanSize = 64
	InitSequence            = uint64(1)
)

// workerEntity worker input
type workerEntity struct {
	sequence   uint64 // batch sequence
	initParser bool
	events     []*binLogEvent
}

func (input *workerEntity) setSequence(sequence uint64) *workerEntity {
	input.sequence = sequence
	return input
}

func (input *workerEntity) addEvent(event *binLogEvent) *workerEntity {
	input.events = append(input.events, event)
	return input
}

func (input *workerEntity) setInitParser(initParser bool) *workerEntity {
	input.initParser = initParser
	return input
}

func (input *workerEntity) reset() {
	input.sequence = 0
	input.initParser = false
	input.events = input.events[:0]
}

// binLogEvent binlog event
type binLogEvent struct {
	data              []byte // binlog event
	enableSemiSyncAck bool   // need semiSync to mysql master
}

func (e *binLogEvent) setData(data []byte) *binLogEvent {
	e.data = data
	return e
}

func (e *binLogEvent) setSemiSyncAck(semiSync bool) *binLogEvent {
	e.enableSemiSyncAck = semiSync
	return e
}

func (e *binLogEvent) reset() *binLogEvent {
	e.data = nil
	e.enableSemiSyncAck = false
	return e
}

// asyncEventProcessor process binlog event with async
type asyncEventProcessor struct {
	sequence            uint64        // keep event order
	*dispatcher                       // dispatch event
	currentWorkerEntity *workerEntity // runtime info
}

// dispatcher dispatch workerEntity to suitable worker
type dispatcher struct {
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	wg            sync.WaitGroup
	locked        int32
	dispatchQueue []*workerEntity // dispatcher info
	*workerManager
}

func (d *dispatcher) addToPendingDispatchQueue(entity *workerEntity) {
	d.mu.Lock()
	d.dispatchQueue = append(d.dispatchQueue, entity)
	d.mu.Unlock()
}

func (d *dispatcher) getPendingDispatchQueue() []*workerEntity {
	res := make([]*workerEntity, 0, 1024)

	d.mu.Lock()
	res = append(res, d.dispatchQueue...)
	d.dispatchQueue = d.dispatchQueue[:0]
	d.mu.Unlock()
	return res
}

func (d *dispatcher) start() {
	d.wg.Add(1)
	go d.dispatch()
	d.workerManager.start()
}

func (d *dispatcher) dispatch() {
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:

			pending := d.getPendingDispatchQueue()

			for _, entity := range pending {
				d.submitToWorker(entity)
			}
			ticker.Reset(10 * time.Millisecond)
		}
	}
}

func (d *dispatcher) close() {
	d.cancel()
	d.wg.Wait()
	d.workerManager.close()
}

// workerManager Manage all workers, who are ultimately responsible for parse binlog
type workerManager struct {
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
	nextSequence             uint64
	concurrency              int // workers info
	workerQueues             []chan *workerEntity
	queueSize                int
	currentWorkerQueue       chan *workerEntity
	nextWorkerIndex          int
	parserTemplate           *BinlogParser              // used to generate parser for every worker
	updatePosAndGTIDCallback func(e *BinlogEvent) error // callback after parsing a binLog event
	replySemiSyncCallback    func() error
	*entityPool              // workerEntity pool
}

func (wm *workerManager) SetUpdatePosAndFTIDCallback(callback func(e *BinlogEvent) error) {
	wm.updatePosAndGTIDCallback = callback
}

func (wm *workerManager) SetReplySemiSyncCallback(callback func() error) {
	wm.replySemiSyncCallback = callback
}

func (wm *workerManager) start() {
	for i := 0; i < wm.concurrency; i++ {
		wm.wg.Add(1)
		wm.workerQueues = append(wm.workerQueues, make(chan *workerEntity, DefaultQueueSize))
		go wm.worker(wm.workerQueues[i])
	}

	wm.currentWorkerQueue = wm.workerQueues[0]
}

func (wm *workerManager) worker(queue chan *workerEntity) {
	//parser := wm.parserTemplate.Clone()

	//pending := make([]*workerEntity, 0, 1024)
	//ticker := time.NewTicker(10 * time.Millisecond)
	//for entity := range queue {
	//	if entity.initParser {
	//		e, err := parser.Parse(entity.events[0].data)
	//		if err != nil {
	//			panic("parse format failed")
	//		}
	//		parser.format = e.Event.(*FormatDescriptionEvent)
	//		// wm.putWorkerEntity(entity)
	//		continue
	//	}
	//
	//	for _, event := range entity.events {
	//		_, _ = parser.Parse(event.data)
	//	}
	//}
	//for {
	//	select {
	//	case <-wm.ctx.Done():
	//		return
	//	case entity := <-queue:
	//		if entity.initParser {
	//			e, err := parser.Parse(entity.events[0].data)
	//			if err != nil {
	//				panic("parse format failed")
	//			}
	//			parser.format = e.Event.(*FormatDescriptionEvent)
	//			// wm.putWorkerEntity(entity)
	//			continue
	//		}
	//
	//		for _, event := range entity.events {
	//			_, _ = parser.Parse(event.data)
	//		}
	//
	//
	//		// pending = append(pending, entity)
	//
	//	//case <-ticker.C:
	//	//
	//	//	if len(pending) == 0 {
	//	//		ticker.Reset(10 * time.Millisecond)
	//	//		continue
	//	//	}
	//	//
	//	//	// increaseStep := 0
	//	//	for _, entity := range pending {
	//	//
	//	//		for _, event := range entity.events {
	//	//			_, _ = parser.Parse(event.data)
	//	//		}
	//	//
	//	//		// wm.putWorkerEntity(entity)
	//	//		continue
	//	//
	//	//		//if entity.sequence != wm.nextSequence {
	//	//		//	ticker.Reset(10 * time.Millisecond)
	//	//		//	continue
	//	//		//}
	//	//		//
	//	//		//for _, event := range entity.events {
	//	//		//
	//	//		//	e, err := parser.Parse(event.data)
	//	//		//	if err != nil {
	//	//		//		panic(fmt.Sprintf("parse normal event failed: %s", err.Error()))
	//	//		//	}
	//	//		//
	//	//		//	if e.Header.EventType == ROTATE_EVENT {
	//	//		//		if string(e.Event.(*RotateEvent).NextLogName) == "mysql-bin.000014" {
	//	//		//			panic("big transaction, return")
	//	//		//		}
	//	//		//
	//	//		//		fmt.Println(string(e.Event.(*RotateEvent).NextLogName))
	//	//		//	}
	//	//		//
	//	//		//	if err := wm.updatePosAndGTIDCallback(e); err != nil {
	//	//		//		panic("update pos failed")
	//	//		//	}
	//	//		//
	//	//		//	if event.enableSemiSyncAck && wm.replySemiSyncCallback != nil {
	//	//		//		if err := wm.replySemiSyncCallback(); err != nil {
	//	//		//			panic("semiSync failed")
	//	//		//		}
	//	//		//	}
	//	//		//}
	//	//		//
	//	//		//increaseStep++
	//	//		//wm.nextSequence++
	//	//		//wm.putWorkerEntity(entity)
	//	//	}
	//	//
	//	//	pending = pending[:0]
	//	//
	//	//	ticker.Reset(10 * time.Millisecond)
	//
	//	}
	//}
}

//func (wm *workerManager) initAllWorkerParser(entity *workerEntity) {
//	for _, worker := range wm.workerQueues {
//		worker <- wm.getWorkerEntity().setInitParser(true).addEvent(wm.getBinlogEvent().setData(entity.events[0].data))
//	}
//}

func (wm *workerManager) initAllWorkerParser(entity *workerEntity) {

	for _, worker := range wm.workerQueues {
		worker <- &workerEntity{
			initParser: true,
			events: []*binLogEvent{
				{
					data: entity.events[0].data,
				},
			},
		}
	}
}

func (wm *workerManager) submitToWorker(entity *workerEntity) {
	if entity.initParser {
		wm.initAllWorkerParser(entity)
	}

	wm.currentWorkerQueue <- entity.setInitParser(false)
	wm.currentWorkerQueue = wm.workerQueues[wm.nextWorkerIndex]
	wm.nextWorkerIndex = (wm.nextWorkerIndex + 1) % wm.concurrency

}

func (wm *workerManager) close() {
	wm.cancel()
	wm.wg.Wait()
}

// entityPool cache workerEntity to avoid frequent creation of objects
type entityPool struct {
	workerEntityPool       *sync.Pool // workerEntity pool
	workerEntityBufferChan chan *workerEntity

	binLogEventPool       *sync.Pool // binLogEvent pool
	binLogEventBufferChan chan *binLogEvent
}

// getWorkerEntity get workerEntity instance from pool
//func (pool *entityPool) getWorkerEntity() (entity *workerEntity) {
//	select {
//	case entity = <-pool.workerEntityBufferChan:
//	default:
//		entity = pool.workerEntityPool.Get().(*workerEntity)
//	}
//
//	entity.reset()
//	return
//}
//
//// putWorkerEntity put workerEntity instance to pool
//func (pool *entityPool) putWorkerEntity(entity *workerEntity) {
//	if entity == nil {
//		return
//	}
//
//	// put event to pool
//	for _, event := range entity.events {
//		pool.putBinlogEvent(event)
//	}
//
//	select {
//	case pool.workerEntityBufferChan <- entity:
//	default:
//		pool.workerEntityPool.Put(entity)
//	}
//}
//
//func (pool *entityPool) getBinlogEvent() (event *binLogEvent) {
//	select {
//	case event = <-pool.binLogEventBufferChan:
//	default:
//		event = pool.binLogEventPool.Get().(*binLogEvent)
//	}
//
//	event.reset()
//	return
//}
//
//func (pool *entityPool) putBinlogEvent(event *binLogEvent) {
//	if event == nil {
//		return
//	}
//
//	select {
//	case pool.binLogEventBufferChan <- event:
//	default:
//		pool.binLogEventPool.Put(event)
//	}
//}

func newAsyncEventProcessor(concurrency int, queueSize int, parser *BinlogParser) *asyncEventProcessor {
	wm := &workerManager{
		wg:              sync.WaitGroup{},
		nextSequence:    InitSequence,
		concurrency:     concurrency,
		queueSize:       queueSize,
		nextWorkerIndex: 1 % concurrency,
		parserTemplate:  parser,
	}

	wm.entityPool = &entityPool{
		workerEntityPool: &sync.Pool{
			New: func() interface{} {
				return &workerEntity{
					events: make([]*binLogEvent, 0, 10),
				}
			},
		},
		workerEntityBufferChan: make(chan *workerEntity, DefaultBufferedChanSize),

		binLogEventPool: &sync.Pool{
			New: func() interface{} {
				return &binLogEvent{}
			},
		},
		binLogEventBufferChan: make(chan *binLogEvent, DefaultBufferedChanSize),
	}

	wm.ctx, wm.cancel = context.WithCancel(context.Background())

	dis := &dispatcher{
		wg:            sync.WaitGroup{},
		locked:        1,
		dispatchQueue: make([]*workerEntity, 0, 1024),
		workerManager: wm,
	}
	dis.ctx, dis.cancel = context.WithCancel(context.Background())

	//p := &asyncEventProcessor{
	//	sequence:            InitSequence,
	//	dispatcher:          dis,
	//	currentWorkerEntity: dis.getWorkerEntity().setSequence(InitSequence),
	//}

	p := &asyncEventProcessor{
		sequence:            InitSequence,
		dispatcher:          dis,
		currentWorkerEntity: &workerEntity{sequence: InitSequence},
	}

	return p
}

func (p *asyncEventProcessor) Start() {
	p.start()
}

func (p *asyncEventProcessor) Close() {
	p.close()
}

// Process receive event binlog
func (p *asyncEventProcessor) Process(eventType EventType, data []byte, enableSemiSyncAck bool) {
	if eventType == FORMAT_DESCRIPTION_EVENT {
		p.processFormatDescriptionEvent(data, enableSemiSyncAck)
		return
	}

	if eventType == XID_EVENT || eventType == HEARTBEAT_EVENT {
		p.processXIDEvent(data, enableSemiSyncAck)
		return
	}

	p.processNormalEvent(data, enableSemiSyncAck)
}

//// processFormatDescriptionEvent need to send this event to all worker to init parser
//func (p *asyncEventProcessor) processFormatDescriptionEvent(data []byte, enableSemiSyncAck bool) {
//	p.addToDispatchQueue()
//	p.currentWorkerEntity.addEvent(p.getBinlogEvent().setData(data).setSemiSyncAck(enableSemiSyncAck)).setInitParser(true)
//	p.addToDispatchQueue()
//}
//
//// processNormalEvent just add to current transaction buffer
//func (p *asyncEventProcessor) processNormalEvent(data []byte, enableSemiSyncAck bool) {
//	p.currentWorkerEntity.addEvent(p.getBinlogEvent().setData(data).setSemiSyncAck(enableSemiSyncAck))
//}
//
//// processXIDEvent send current transaction to current worker and change worker
//func (p *asyncEventProcessor) processXIDEvent(data []byte, enableSemiSyncAck bool) {
//	p.currentWorkerEntity.addEvent(p.getBinlogEvent().setData(data).setSemiSyncAck(enableSemiSyncAck))
//	p.addToDispatchQueue()
//}

// addToDispatchQueue send currentWorkerEntity to dispatcher
//func (p *asyncEventProcessor) addToDispatchQueue() {
//
//	p.addToPendingDispatchQueue(p.currentWorkerEntity)
//
//	p.sequence++
//	p.currentWorkerEntity = p.getWorkerEntity().setSequence(p.sequence)
//
//}

// processFormatDescriptionEvent need to send this event to all worker to init parser
func (p *asyncEventProcessor) processFormatDescriptionEvent(data []byte, enableSemiSyncAck bool) {
	p.addToDispatchQueue()
	p.currentWorkerEntity.addEvent(&binLogEvent{
		data:              data,
		enableSemiSyncAck: enableSemiSyncAck,
	}).setInitParser(true)
	p.addToDispatchQueue()
}

// processNormalEvent just add to current transaction buffer
func (p *asyncEventProcessor) processNormalEvent(data []byte, enableSemiSyncAck bool) {
	p.currentWorkerEntity.addEvent(&binLogEvent{
		data:              data,
		enableSemiSyncAck: enableSemiSyncAck,
	})
}

// processXIDEvent send current transaction to current worker and change worker
func (p *asyncEventProcessor) processXIDEvent(data []byte, enableSemiSyncAck bool) {
	p.currentWorkerEntity.addEvent(&binLogEvent{
		data:              data,
		enableSemiSyncAck: enableSemiSyncAck,
	})
	p.addToDispatchQueue()
}

func (p *asyncEventProcessor) addToDispatchQueue() {

	p.addToPendingDispatchQueue(p.currentWorkerEntity)

	p.sequence++
	p.currentWorkerEntity = &workerEntity{sequence: p.sequence}

}
