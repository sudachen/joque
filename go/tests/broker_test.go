package tests

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/glog"
	. "github.com/sudachen/joque/go/broker"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

var _uid int64 = 1

func UID() int64 {
	_uid++
	return _uid
}

type WorkerAssert interface {
	OnExec(Job, *testing.T)
}

type _Worker struct {
	id       int64
	t        *testing.T
	chExec   chan Job
	chDiscon chan int
	assert   WorkerAssert
}

func (wrk *_Worker) ID() int64 {
	return wrk.id
}
func (wrk *_Worker) Execute(j Job) {
	glog.Infof("_Worker.Execute(%d)", j.ID())
	wrk.chExec <- j
}
func (wrk *_Worker) Disconnect() {
	wrk.chDiscon <- 1
	glog.Infof("waiting disconnect worker %d", wrk.id)
	_ = <-wrk.chDiscon
	glog.Infof("worker %d disconnected", wrk.id)
}

func StartWorker(topic string, testset []*TestJob, brk Broker, t *testing.T, assert WorkerAssert) {
	wrk := &_Worker{id: UID(), t: t, chExec: make(chan Job), chDiscon: make(chan int), assert: assert}
	tj := make(map[string]*TestJob)
	for _, j := range testset {
		tj[j.message] = j
	}
	go func() {
		brk.Subscribe(wrk, topic)
	loop:
		for {
			select {
			case j := <-wrk.chExec:
				message := string(j.Payload())
				glog.Infof("chExec -> %d, %s", j.ID(), message)
				if tj[message].id != j.ID() {
					t.Fail()
				}
				if wrk.assert != nil {
					wrk.assert.OnExec(j, wrk.t)
				}
				if j.QoS() == QosComplete {
					j.SetResult([]byte(tj[message].result))
				}
				j.Success()
				brk.Complete(j, wrk)
			case <-wrk.chDiscon:
				close(wrk.chExec)
				close(wrk.chDiscon)
				break loop
			}
		}
	}()
	return
}

type OriginatorAssert interface {
	OnComplete(Job, *testing.T, map[int64]*TestJob)
}

type _Originator struct {
	id         int64
	t          *testing.T
	chDone     chan struct{}
	chComplete chan struct{}
	tj         map[int64]*TestJob
	mu         sync.Mutex
	assert     OriginatorAssert
}

func StartOriginator(t *testing.T, assert OriginatorAssert) (org *_Originator) {
	org = &_Originator{id: UID(), t: t,
		chDone:     make(chan struct{}),
		chComplete: make(chan struct{}),
		assert:     assert}
	//go func() {
	//}()
	return
}

func (org *_Originator) Empty() (empty bool) {
	org.mu.Lock()
	empty = len(org.tj) == 0
	org.mu.Unlock()
	return
}

func (org *_Originator) Go(testset []*TestJob, brk Broker) {
	org.tj = make(map[int64]*TestJob)
	go func() {
		glog.Infof("processing testset on originator %d", org.id)
		for _, tj := range testset {
			j := tj.ToJob(org.t)
			glog.Infof("QoS %d", j.QoS())
			if j.QoS() >= QosAck {
				org.mu.Lock()
				org.tj[j.ID()] = tj
				org.mu.Unlock()
			}
			glog.Infof("%d -> %v", j.ID(), org.tj[j.ID()])
			brk.Enqueue(j, org)
		}
		for !testset[len(testset)-1].ack {
			time.Sleep(10 * time.Millisecond)
		}
		close(org.chDone)
		for !org.Empty() {
			time.Sleep(10 * time.Millisecond)
		}
		close(org.chComplete)
	}()
}

func (org *_Originator) Done() {
	_ = <-org.chDone
}

func (org *_Originator) Join() {
	_ = <-org.chComplete
}

func (org *_Originator) ID() int64 {
	return org.id
}

func (org *_Originator) Acknowledge(j Job) {
	glog.Infof("acknowledge on job %d", j.ID())
	org.mu.Lock()
	glog.Infof("%v", org.tj[j.ID()])
	if tj := org.tj[j.ID()]; tj != nil {
		tj.ack = true
		if tj.qos < QosComplete {
			delete(org.tj, j.ID())
		}
	} else {
		org.t.Errorf("acknowledge on job %d is not welcome", j.ID())
		org.t.Fail()
	}
	org.mu.Unlock()
}

func (org *_Originator) Complete(j Job) {
	glog.Infof("complete on job %d", j.ID())
	org.mu.Lock()
	if org.assert != nil {
		org.assert.OnComplete(j, org.t, org.tj)
	}
	if tj := org.tj[j.ID()]; tj != nil {
		tj.complete = true
		if tj.result != string(j.Result()) {
			org.t.Errorf("complete on job %d failed with invalid job result", j.ID())
			org.t.Fail()
		}
		delete(org.tj, j.ID())
	} else {
		org.t.Errorf("complete on job %d is not welcome", j.ID())
		org.t.Fail()
	}
	org.mu.Unlock()
}

func (org *_Originator) Disconnect() {
	glog.Infof("disconnect originator %d", org.id)
}

type TestJob struct {
	id       int64
	topic    string
	qos      int
	priority int
	ttl      int
	message  string
	ack      bool
	complete bool
	result   string
}

func NewTestJob(topic string, qos int, priority int, ttl int, message string, result string) (tj *TestJob) {
	tj = &TestJob{0, topic, qos, priority, ttl, message, false, false, result}
	return
}

func (tj *TestJob) ToJob(t *testing.T) (j Job) {
	var err error
	if tj.id != 0 {
		t.Fail()
	}
	j, err = NewJob(tj.topic, []byte(tj.message), tj.priority, tj.ttl, tj.qos)
	if err != nil {
		t.Fail()
	}
	tj.id = j.ID()
	return
}

func TestStartStop(t *testing.T) {
	brk := StartJoqueBroker()
	glog.Info("started?")
	brk.Stop()
}

func MakeTestset1(count int) (tj []*TestJob) {
	tj = make([]*TestJob, count)
	for i := 0; i < count; i++ {
		tj[i] = NewTestJob("test", QosAck, PriorityHigh, 1, "test message "+strconv.Itoa(i), strconv.Itoa(i))
	}
	return
}

func TestJobSet1(t *testing.T) {
	testset := MakeTestset1(100)
	brk := StartJoqueBroker()
	StartWorker("test", testset, brk, t, nil)
	StartWorker("test", testset, brk, t, nil)
	org := StartOriginator(t, nil)
	org.Go(testset, brk)
	org.Join()
	brk.Stop()
}

func RandomPriority() int {
	return PriorityHigh + rand.Intn(PriorityCount)
}

func MakeTestsetPriority(count int) (tj []*TestJob) {
	tj = make([]*TestJob, count)
	for i := 0; i < count; i++ {
		tj[i] = NewTestJob("test", QosComplete, RandomPriority(), 1, "test message "+strconv.Itoa(i), strconv.Itoa(i))
	}
	return
}

type _PriorityAssert struct {
	last int
}

func (a *_PriorityAssert) OnComplete(j Job, t *testing.T, tj map[int64]*TestJob) {
	if j.Priority() < a.last {
		t.Errorf("oncomplete priority assert: curr job %d priority %d is higher then %d", j.ID(), j.Priority(), a.last)
		t.Fail()
	}
	a.last = j.Priority()
}

func (a *_PriorityAssert) OnExec(j Job, t *testing.T) {
	if j.Priority() < a.last {
		t.Errorf("onexec priority assert: curr job %d priority %d is higher then %d", j.ID(), j.Priority(), a.last)
		t.Fail()
	}
	a.last = j.Priority()
}

func TestJobPriority1(t *testing.T) {
	testset := MakeTestsetPriority(10)
	brk := StartJoqueBroker()
	org := StartOriginator(t, &_PriorityAssert{})
	org.Go(testset, brk)
	org.Done()
	StartWorker("test", testset, brk, t, &_PriorityAssert{})
	org.Join()
	brk.Stop()
}

func TestJobPriority2(t *testing.T) {
	testset := MakeTestsetPriority(10000)
	brk := StartJoqueBroker()
	// I can't check priority on complete because order of complete
	//   from several workers is not determinated
	org := StartOriginator(t, nil)
	org.Go(testset, brk)
	org.Done()
	StartWorker("test", testset, brk, t, &_PriorityAssert{})
	StartWorker("test", testset, brk, t, &_PriorityAssert{})
	StartWorker("test", testset, brk, t, &_PriorityAssert{})
	org.Join()
	brk.Stop()
}
