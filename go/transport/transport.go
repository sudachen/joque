package transport

import (
	"errors"
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
	// MqQuit identifies QUIT message
	MqQuit
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

// MQ is the message in-out queue
type MQ struct {
	In  chan *Message
	Out chan *Message
}

// Close queue channels
func (mq *MQ) Close() {
	close(mq.In)
	close(mq.Out)
}

// Send message to client
func (mq *MQ) Send(m *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("closed")
		}
	}()
	mq.In <- m
	return
}

// MqtUpgrade upgrades network Conn to Message chanel
func MqtUpgrade(rw interface{}, mqt MQT) (mq *MQ) {
	mq = &MQ{make(chan *Message, 1), make(chan *Message, 1)}
	go func() {
		rd := rw.(io.Reader)
		for {
			m, err := mqt.MqtRead(rd)
			if err != nil {
				if err != io.EOF {
					glog.Errorf("failed to read message: %s", err.Error())
				}
				mq.Send(nil)
				return
			}
			mq.Send(m)
		}
	}()
	go func() {
		wr := rw.(io.Writer)
		for {
			select {
			case m, ok := <-mq.Out:
				if !ok {
					return
				}
				err := mqt.MqtWrite(wr, m)
				if err != nil {
					glog.Errorf("failed to write message: %s", err.Error())
					mq.Send(nil)
					return
				}
			}
		}
	}()
	return
}
