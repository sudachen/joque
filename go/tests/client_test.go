package tests

import (
	"bytes"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/golang/glog"
	. "github.com/sudachen/joque/go/broker"
	. "github.com/sudachen/joque/go/client"
	. "github.com/sudachen/joque/go/transport"
)

type _JobNfo struct {
	job Job
	org Originator
}

type _UpperBroker struct {
	chEnque chan _JobNfo
	chStop  chan int
}

func (brk *_UpperBroker) Enqueue(job Job, org Originator) (err error) {
	glog.Infof("enqueue job %d", job.ID())
	brk.chEnque <- _JobNfo{job, org}
	return
}

func (brk *_UpperBroker) Complete(Job, Worker) (err error) {
	return
}

func (brk *_UpperBroker) Subscribe(Worker, string) (err error) {
	return
}

func (brk *_UpperBroker) Unsubscribe(Worker, bool) (err error) {
	return
}

func (brk *_UpperBroker) Stop() (err error) {
	brk.chStop <- 0
	_ = <-brk.chStop
	return
}

func StartUpperBroker() Broker {
	brk := &_UpperBroker{
		make(chan _JobNfo, 3),
		make(chan int),
	}
	c := make(chan struct{})
	go func() {
		close(c)
		for {
			select {
			case j := <-brk.chEnque:
				glog.Infof("upper broker: ack on job %d", j.job.ID())
				j.org.Acknowledge(j.job)
				payload := j.job.Payload()
				if payload != nil {
					payload = []byte(strings.ToUpper(string(payload)))
					glog.Infof("upper broker: set result on job %d", j.job.ID())
					j.job.SetResult(payload)
				}
				glog.Infof("upper broker: success on job %d", j.job.ID())
				j.job.Success()
				glog.Infof("upper broker: complete on job %d", j.job.ID())
				j.org.Complete(j.job)
			case <-brk.chStop:
				close(brk.chEnque)
				close(brk.chStop)
				return
			}
		}
	}()
	_ = <-c
	return brk
}

type _UpperWorker struct {
	id  int64
	brk Broker
}

func (wrk *_UpperWorker) ID() int64 {
	return wrk.id
}
func (wrk *_UpperWorker) Execute(job Job) {
	go func() {
		glog.Infof("upper worker: execute %d", job.ID())
		payload := job.Payload()
		if payload != nil {
			payload = []byte(strings.ToUpper(string(payload)))
			glog.Infof("upper worker: set result on job %d", job.ID())
			job.SetResult(payload)
		}
		glog.Infof("upper worker: success on job %d", job.ID())
		job.Success()
		glog.Infof("upper worker: complete on job %d", job.ID())
		wrk.brk.Complete(job, wrk)
	}()
}

func (wrk *_UpperWorker) Disconnect() {

}

type _IO struct {
	in  chan byte
	out chan byte
}

func (sio *_IO) Read(p []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = io.EOF
		}
	}()
	n = 1
	p[0] = <-sio.in
	return
}

func (sio *_IO) Write(p []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = io.EOF
		}
	}()
	n = 0
	for _, c := range p {
		sio.out <- c
		n++
	}
	return
}

func (sio *_IO) WriteMesg(cmd string) {
	for _, c := range []byte(cmd) {
		sio.in <- c
	}
}

func (sio *_IO) ReadMesg() string {
	var bf bytes.Buffer
	nl := false
	for {
		c := <-sio.out
		bf.WriteByte(c)
		if c == '\n' {
			if nl {
				return bf.String()
			}
			nl = true
		} else {
			nl = false
		}
	}
}

func _SimpleT1(sio *_IO, t *testing.T) {
	sio.WriteMesg("0 CONNECT\n\n")
	if msg := sio.ReadMesg(); msg != "0 ACK\n\n" {
		t.Error("on connect ack")
	}
	sio.WriteMesg("10 PUBLISH test 2 1\ntest payload\n\n")
	if msg := sio.ReadMesg(); msg != "10 ACK\n\n" {
		t.Error("on publish ack")
	}
	sio.WriteMesg("20 PUBLISH test 1 1\ntest payload\n\n")
	if msg := sio.ReadMesg(); msg != "20 ACK\n\n" {
		t.Error("on publish ack")
	}
	sio.WriteMesg("30 PUBLISH test 0 1\ntest payload\n\n")
	sio.WriteMesg("10 QUERY\n\n")
loop1:
	for {
		msg := sio.ReadMesg()
		switch msg {
		case "10 ACK\n\n":
		case "10 COMPLETE\nTEST PAYLOAD\n\n":
			break loop1
		default:
			t.Error("on complete 1")
			break loop1
		}
	}
	sio.WriteMesg("20 QUERY\n\n")
	if msg := sio.ReadMesg(); msg != "20 ACK\n\n" {
		t.Error("on query ack 20")
	}
	sio.WriteMesg("30 QUERY\n\n")
	if msg := sio.ReadMesg(); msg != "30 ACK\n\n" {
		t.Error("on query ack 30")
	}
	sio.WriteMesg("0 QUIT\n\n")
}

func _SimpleT11(sio *_IO, t *testing.T) {
	sio.WriteMesg("0 CONNECT\n\n")
	if msg := sio.ReadMesg(); msg != "0 ACK\n\n" {
		t.Error("X: on connect ack")
	}
	sio.WriteMesg("0 SUBSCRIBE test\n\n")
	if msg := sio.ReadMesg(); msg != "0 ACK\n\n" {
		t.Error("X: on publish ack")
	}
	glog.Info("X: worker subscribed")
	rx, _ := regexp.Compile("(\\d+) PUBLISH test \\d \\d\ntest payload\n\n")
	if match := rx.FindStringSubmatch(sio.ReadMesg()); match != nil {
		sio.WriteMesg(match[1] + " COMPLETE\nTEST PAYLOAD\n\n")
		glog.Info("X: worker job " + match[1])
	} else {
		t.Fail()
	}
	if match := rx.FindStringSubmatch(sio.ReadMesg()); match != nil {
		sio.WriteMesg(match[1] + " COMPLETE\nTEST PAYLOAD\n\n")
		glog.Info("X: worker job " + match[1])
	} else {
		t.Fail()
	}
	if match := rx.FindStringSubmatch(sio.ReadMesg()); match != nil {
		sio.WriteMesg(match[1] + " COMPLETE\nTEST PAYLOAD\n\n")
		glog.Info("X: worker job " + match[1])
	} else {
		t.Fail()
	}
	sio.WriteMesg("0 QUIT\n\n")
}

func TestClientOriginator1(t *testing.T) {
	brk := StartUpperBroker()
	sio := &_IO{make(chan byte), make(chan byte)}
	mq := MqtUpgrade(sio, &ASCIIMqt{})
	Connect(mq, brk, 1)
	_SimpleT1(sio, t)
}

func TestClientOriginator2(t *testing.T) {
	brk := StartJoqueBroker()
	sio := &_IO{make(chan byte, 256), make(chan byte, 256)}
	mq := MqtUpgrade(sio, &ASCIIMqt{})
	wrk := &_UpperWorker{NextID(), brk}
	Connect(mq, brk, 1)
	brk.Subscribe(wrk, "test")
	_SimpleT1(sio, t)
}

func TestClient1(t *testing.T) {
	brk := StartJoqueBroker()
	sio1 := &_IO{make(chan byte, 256), make(chan byte, 256)}
	sio2 := &_IO{make(chan byte, 256), make(chan byte, 256)}
	mq1 := MqtUpgrade(sio1, &ASCIIMqt{})
	mq2 := MqtUpgrade(sio2, &ASCIIMqt{})
	Connect(mq1, brk, 1)
	Connect(mq2, brk, 1)
	go _SimpleT11(sio2, t)
	_SimpleT1(sio1, t)
	brk.Stop()
}
