package broker

import (
	"errors"

	"github.com/golang/glog"
)

type _Job struct {
	id        int64
	result    []byte
	succeeded bool
	topic     string
	payload   []byte
	qos       int
	ttl       int
	priority  int
}

func (job *_Job) ID() int64 {
	return job.id
}

func (job *_Job) Result() []byte {
	return job.result
}

func (job *_Job) SetResult(result []byte) {
	job.result = result
}

func (job *_Job) Success() {
	job.succeeded = true
}

func (job *_Job) IsSucceeded() bool {
	return job.succeeded
}

func (job *_Job) Topic() string {
	return job.topic
}

func (job *_Job) Payload() []byte {
	return job.payload
}

func (job *_Job) QoS() int {
	return job.qos
}

func (job *_Job) CanRetryWithTTL() bool {
	if job.ttl > 0 {
		job.ttl--
		return true
	}
	return false
}

func (job *_Job) Priority() int {
	return job.priority
}

var _uid int64

// NewJob creates new Job object
func NewJob(topic string, payload []byte, priority int, ttl int, qos int) (job Job, err error) {
	if qos < QosAck || qos > QosComplete {
		glog.Errorf("invalid Job QoS value %d", qos)
		err = errors.New("invalid Job QoS value")
		return
	}
	if priority < PriorityHigh || priority > PriorityLow {
		glog.Errorf("invalid Job priority %d", priority)
		err = errors.New("invalid Job priority")
		return
	}
	_uid++
	job = &_Job{
		id:        _uid,
		topic:     topic,
		payload:   payload,
		priority:  priority,
		ttl:       ttl,
		qos:       qos,
		succeeded: false,
	}
	return
}
