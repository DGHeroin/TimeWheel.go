package TimeWheel

import (
    "container/list"
    "time"
)

type (
    TimeJob  func(key interface{}, value interface{})
    TimeTask struct {
        delay    time.Duration
        circle   int
        key      interface{}
        data     interface{}
        isActive bool
    }
    TimeWheel struct {
        tickInterval    time.Duration
        ticker          *time.Ticker
        slots           []*list.List
        timer           map[interface{}]int
        currentPosition int
        slotNum         int
        job             TimeJob
        addTaskChan     chan *TimeTask
        removeTaskChan  chan interface{}
        stopChan        chan struct{}
    }
)

func NewTimeWheel(interval time.Duration, slotNum int, job TimeJob) *TimeWheel {
    if interval <= 0 || slotNum <= 0 || job == nil {
        return nil
    }
    t := &TimeWheel{
        tickInterval:    interval,
        slots:           make([]*list.List, slotNum),
        timer:           make(map[interface{}]int),
        currentPosition: 0,
        job:             job,
        slotNum:         slotNum,
        addTaskChan:     make(chan *TimeTask),
        removeTaskChan:  make(chan interface{}),
        stopChan:        make(chan struct{}),
    }
    t.initSlots()
    return t
}

func (w *TimeWheel) initSlots() {
    for i := 0; i < w.slotNum; i++ {
        w.slots[i] = list.New()
    }
}
func (w *TimeWheel) Start() {
    w.ticker = time.NewTicker(w.tickInterval)
    go func() {
        for {
            select {
            case <-w.ticker.C:
                w.tickHandler()
            case task := <-w.addTaskChan:
                w.addTask(task)
            case key := <-w.removeTaskChan:
                w.removeTask(key)
            case <-w.stopChan:
                w.ticker.Stop()
                return
            }
        }
    }()
}
func (w *TimeWheel) AddTimer(delay time.Duration, key interface{}, data interface{}) {
    if delay <= 0 {
        return
    }
    w.addTaskChan <- &TimeTask{
        delay:    delay,
        key:      key,
        data:     data,
        isActive: true,
    }
}
func (w *TimeWheel) RemoveTimer(key interface{}) {
    if key == nil {
        return
    }
    w.removeTaskChan <- key
}

func (w *TimeWheel) tickHandler() {
    l := w.slots[w.currentPosition]
    w.scanAndRunTask(l)
    if w.currentPosition == w.slotNum-1 {
        w.currentPosition = 0
    } else {
        w.currentPosition++
    }
}

func (w *TimeWheel) addTask(task *TimeTask) {
    pos, circle := w.getPositionAndCircle(task.delay)
    task.circle = circle
    w.slots[pos].PushBack(task)
    if task.key != nil {
        w.timer[task.key] = pos
    }
}

func (w *TimeWheel) getPositionAndCircle(d time.Duration) (int, int) {
    delaySec := int(d.Seconds())
    tvSec := int(w.tickInterval.Seconds())
    circle := delaySec / tvSec / w.slotNum
    pos := (w.currentPosition + delaySec/tvSec) % w.slotNum
    return pos, circle
}

func (w *TimeWheel) removeTask(key interface{}) {
    pos, ok := w.timer[key]
    if !ok {
        return
    }
    l := w.slots[pos]
    for e := l.Front(); e != nil; {
        task := e.Value.(*TimeTask)
        if task.key == key {
            task.isActive = false
            delete(w.timer, task.key)
            l.Remove(e)
        }
        e = e.Next()
    }
    w.slots[pos] = l
}

func (w *TimeWheel) scanAndRunTask(l *list.List) {
    for e := l.Front(); e != nil; {
        task := e.Value.(*TimeTask)
        if task.circle > 0 {
            task.circle--
            e = e.Next()
            continue
        }
        if task.isActive {
            go w.job(task.key, task.data)
        }
        next := e.Next()
        l.Remove(e)
        if task.key != nil {
            delete(w.timer, task.key)
        }
        e = next
    }
}

func (w *TimeWheel) Stop() {
    w.stopChan <- struct{}{}
}
