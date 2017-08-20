package server

import (
	"math"

	"github.com/golang/glog"
	"github.com/sudachen/joque/go/broker"
	"github.com/sudachen/joque/go/transport"
)

type _Worker struct {
	id           int64
	chExec       chan broker.Job
	chDisconnect chan int
}

type _Originator struct {
	id            int64
	chComplete    chan broker.Job
	chAcknowledge chan broker.Job
	chDisconnect  chan int
}

func (org *_Originator) ID() int64 {
	return org.id
}

func (org *_Originator) Acknowledge(job broker.Job) {
	org.chAcknowledge <- job
}

func (org *_Originator) Complete(job broker.Job) {
	org.chComplete <- job
}

func (org *_Originator) Disconnect() {
	org.chDisconnect <- 0
}

func (wrk *_Worker) ID() int64 {
	return wrk.id
}

func (wrk *_Worker) Execute(job broker.Job) {
	wrk.chExec <- job
}

func (wrk *_Worker) Disconnect() {
	wrk.chDisconnect <- 0
}

// Connect connects message queue and broker
func Connect(mq *transport.MQ, brk broker.Broker, maxTTL int) (err error) {
	c := make(chan error)
	if maxTTL <= 0 {
		maxTTL = math.MaxInt32
	}
	go func() {
		results := make(map[int64]broker.Job)
		jobs := make(map[int64]broker.Job)

		chComplete := make(chan broker.Job, 1)
		chAcknowledge := make(chan broker.Job, 1)
		chExec := make(chan broker.Job, 1)
		chDisconnect := make(chan int, 1)

		defer func() {
			mq.Close()
			close(chComplete)
			close(chAcknowledge)
			close(chExec)
			close(chDisconnect)
		}()

		var org *_Originator
		var wrk *_Worker

		c <- nil
		close(c)

		for {
			select {
			case job := <-chAcknowledge:
				glog.Infof("ack on job %d", job.ID())
				if job.QoS() >= broker.QosAck {
					m := &transport.Message{
						ID:   job.(broker.MesgJob).MesgID(),
						Kind: transport.MqAck}
					mq.Out <- m
				}
			case job := <-chComplete:
				glog.Infof("complete on job %d", job.ID())
				if job.QoS() >= broker.QosComplete {
					glog.Infof("set result job %d for msgid %d", job.ID(), job.(broker.MesgJob).MesgID())
					results[job.(broker.MesgJob).MesgID()] = job
				}
			case job := <-chExec:
				m := &transport.Message{
					ID:       job.ID(),
					Kind:     transport.MqPublish,
					Topic:    job.Topic(),
					Payload:  job.Payload(),
					QoS:      job.QoS(),
					Priority: job.Priority(),
				}
				mq.Out <- m
				jobs[job.ID()] = job
			case m := <-mq.In:
				glog.Infof("msg %v", m)
				if m == nil {
					if wrk != nil {
						brk.Unsubscribe(wrk, true)
					}
					return
				}
				switch m.Kind {
				case transport.MqConnect:
					m := &transport.Message{
						ID:   m.ID,
						Kind: transport.MqAck}
					mq.Out <- m
				case transport.MqPublish:
					if org == nil {
						org = &_Originator{
							broker.NextID(),
							chComplete,
							chAcknowledge,
							chDisconnect,
						}
					}
					var ttl = m.TTL
					if maxTTL < ttl {
						ttl = maxTTL
					}
					job, err := broker.NewJob(m.Topic, m.Payload, m.Priority, ttl, m.QoS)
					if err != nil {
						glog.Errorf("failed to create new job: %s", err.Error())
						return
					}
					job.(broker.MesgJob).SetMesgID(m.ID)
					err = brk.Enqueue(job, org)
					if err != nil {
						glog.Errorf("failed to enqueue job: %s", err.Error())
						return
					}
				case transport.MqSubscribe:
					if wrk == nil {
						wrk = &_Worker{
							broker.NextID(),
							chExec,
							chDisconnect,
						}
					} else {
						glog.Errorf("worker %d already subscribed", wrk.id)
						return
					}
					brk.Subscribe(wrk, m.Topic)
					m := &transport.Message{
						ID:   m.ID,
						Kind: transport.MqAck}
					mq.Out <- m
				case transport.MqQuery:
					if job := results[m.ID]; job != nil {
						m := &transport.Message{
							ID:      m.ID,
							Kind:    transport.MqComplete,
							Payload: job.Result(),
						}
						mq.Out <- m
					} else {
						m := &transport.Message{
							ID:   m.ID,
							Kind: transport.MqAck}
						mq.Out <- m
					}
				case transport.MqComplete:
					if wrk != nil {
						if job := jobs[m.ID]; job != nil {
							if job.QoS() >= broker.QosComplete {
								job.SetResult(m.Payload)
								results[job.(broker.MesgJob).MesgID()] = job
							}
							job.Success()
							delete(jobs, m.ID)
							brk.Complete(job, wrk)
						}
					}
				case transport.MqAck:
					if wrk != nil {
						if job := jobs[m.ID]; job != nil {
							if job.QoS() >= broker.QosComplete {
								results[job.(broker.MesgJob).MesgID()] = job
							}
							job.Success()
							delete(jobs, m.ID)
							brk.Complete(job, wrk)
						}
					}
				case transport.MqQuit:
					if wrk != nil {
						brk.Unsubscribe(wrk, false)
					}
					return
				}
			}
		}
	}()
	err = <-c
	return
}
