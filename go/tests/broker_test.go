package broker_test

import (
	"testing"

	"github.com/golang/glog"
	. "github.com/sudachen/joque/go/broker"
)

var _uid int64 = 1

func UID() int64 {
	_uid++
	return _uid
}

type _Worker struct {
	id       int64
	t        *testing.T
	chExec   chan Job
	chDiscon chan int
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

func StartWorker(topic string, testset []*TestJob, brk Broker, t *testing.T) {
	wrk := &_Worker{id: UID(), t: t, chExec: make(chan Job), chDiscon: make(chan int)}
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

type _Originator struct {
	id     int64
	t      *testing.T
	chDone chan struct{}
}

func StartOriginator(t *testing.T) (org *_Originator) {
	org = &_Originator{id: UID(), t: t, chDone: make(chan struct{})}
	//go func() {
	//}()
	return
}

func (org *_Originator) Go(testset []*TestJob, brk Broker) {
	go func() {
		glog.Infof("processing testset on originator %d", org.id)
		for _, tj := range testset {
			j := tj.ToJob(org.t)
			glog.Infof("enqueue job %d %s", tj.id, tj.message)
			brk.Enqueue(j, org)
		}
		close(org.chDone)
	}()
}

func (org *_Originator) Join() {
	_ = <-org.chDone
}

func (org *_Originator) ID() int64 {
	return org.id
}

func (org *_Originator) Acknowledge(j Job) {
	org.t.Logf("acknowledge on job %d", j.ID())
}

func (org *_Originator) Complete(j Job) {
	org.t.Logf("complete on job %d", j.ID())
}

func (org *_Originator) Disconnect() {
	org.t.Logf("disconnect originator %d", org.id)
}

type TestJob struct {
	id        int64
	topic     string
	qos       int
	priority  int
	ttl       int
	message   string
	processed bool
}

func NewTestJob(topic string, qos int, priority int, ttl int, message string) (tj *TestJob) {
	tj = &TestJob{0, topic, qos, priority, ttl, message, false}
	return
}

func (tj *TestJob) ToJob(t *testing.T) (j Job) {
	var err error
	if tj.id != 0 {
		t.FailNow()
	}
	j, err = NewJob(tj.topic, []byte(tj.message), tj.priority, tj.ttl, tj.qos)
	if err != nil {
		t.FailNow()
	}
	tj.id = j.ID()
	return
}

func TestStartStop(t *testing.T) {
	brk := StartJoqueBroker()
	glog.Info("started?")
	brk.Stop()
}

func MakeTestset1() []*TestJob {
	return []*TestJob{
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 1"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 2"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 3"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 4"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 5"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 6"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 7"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 8"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 9"),
		NewTestJob("test", QosAck, PriorityHigh, 1, "test message 10"),
	}
}

func TestJobSet1(t *testing.T) {
	testset := MakeTestset1()
	brk := StartJoqueBroker()
	StartWorker("test", testset, brk, t)
	StartWorker("test", testset, brk, t)
	org := StartOriginator(t)
	org.Go(testset, brk)
	org.Join()
	brk.Stop()
}
