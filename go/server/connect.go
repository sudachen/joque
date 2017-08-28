package server

import (
	"math"

	"github.com/golang/glog"
	"github.com/sudachen/joque/go/broker"
	"github.com/sudachen/joque/go/transport"
)

type connection struct {
	id           int64
	cExec        chan broker.Job
	cComplete    chan broker.Job
	cAcknowledge chan broker.Job
	cDisconnect  chan int
}

func (conn *connection) ID() int64 {
	return conn.id
}

func (conn *connection) Acknowledge(job broker.Job) {
	conn.cAcknowledge <- job
}

func (conn *connection) Complete(job broker.Job) {
	conn.cComplete <- job
}

func (conn *connection) Execute(job broker.Job) {
	conn.cExec <- job
}

func (c *connection) Disconnect() {
	c.cDisconnect <- 0
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

		conn := connection{
			id:           broker.NextID(),
			cComplete:    make(chan broker.Job, 1),
			cAcknowledge: make(chan broker.Job, 1),
			cExec:        make(chan broker.Job, 1),
			cDisconnect:  make(chan int, 1),
		}

		defer func() {
			mq.Close()
			close(conn.cComplete)
			close(conn.cAcknowledge)
			close(conn.cExec)
			close(conn.cDisconnect)
		}()

		var wrk broker.Worker

		c <- nil

		for {
			select {
			case job := <-conn.cAcknowledge:
				glog.Infof("ack on job %d", job.ID())
				if job.QoS() >= broker.QosAck {
					var payload []byte
					if job.IsRejected() {
						payload = []byte("REJECTED")
					}
					m := &transport.Message{
						ID:      job.(broker.MesgJob).MesgID(),
						Kind:    transport.MqAck,
						Payload: payload,
					}
					mq.Out <- m
				}
			case job := <-conn.cComplete:
				glog.Infof("complete on job %d", job.ID())
				if job.QoS() >= broker.QosComplete {
					glog.Infof("set result job %d for msgid %d", job.ID(), job.(broker.MesgJob).MesgID())
					results[job.(broker.MesgJob).MesgID()] = job
				}
			case job := <-conn.cExec:
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
			case <-conn.cDisconnect:
				if wrk != nil {
					return
				}
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
					err = brk.Enqueue(job, &conn)
					if err != nil {
						glog.Errorf("failed to enqueue job: %s", err.Error())
						return
					}
				case transport.MqSubscribe:
					if wrk == nil {
						wrk = &conn
					} else {
						glog.Errorf("worker %d already subscribed", wrk.ID())
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

	// waiting until goroutine be started successful or failed during starting
	err = <-c
	close(c)

	return
}
