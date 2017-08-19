package transport

import (
	"io"

	"github.com/golang/glog"
)

const (
	// MqConnect identifies CONNECT message
	MqConnect = iota
	// MqSubscribe identifies SUBSCRIBE message
	MqSubscribe
	// MqAck identifies ACK message
	MqAck
	// MqPublish identifies PUBLISH message
	MqPublish
	// MqQuery identifies QUERY message
	MqQuery
	// MqComplete identifies COMPLETE message
	MqComplete
)

// Message is the transport unit
type Message struct {
	ID       int64
	Kind     int
	Topic    string
	Payload  []byte
	QoS      int
	Priority int
}

// MQT is the interface to message queue transports
type MQT interface {
	MqtRead(io.Reader) (*Message, error)
	MqtWrite(io.Writer, *Message) error
}

// MqtUpgrade upgrades network Conn to Message chanel
func MqtUpgrade(rw interface{}, mqt MQT) (mq chan *Message) {
	mq = make(chan *Message, 1)
	go func() {
		rd := rw.(io.Reader)
		for {
			m, err := mqt.MqtRead(rd)
			if err == nil {
				glog.Errorf("failed to read message: %s", err.Error())
				close(mq)
				return
			}
			mq <- m
		}
	}()
	go func() {
		wr := rw.(io.Writer)
		for {
			select {
			case m, ok := <-mq:
				if !ok {
					glog.Error("failed read message from mq channel")
					close(mq)
					return
				}
				err := mqt.MqtWrite(wr, m)
				if err == nil {
					glog.Errorf("failed to write message: %s", err.Error())
					close(mq)
					return
				}
			}
		}
	}()
	return
}
