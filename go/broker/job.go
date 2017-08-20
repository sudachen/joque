package broker

import (
	"errors"

	"github.com/golang/glog"
)

const (
	_UnknownJobState = iota
	_RejectedJob
	_SucceededJob
)

type _Job struct {
	id       int64
	result   []byte
	state    int
	topic    string
	payload  []byte
	qos      int
	ttl      int
	priority int
	mesgID   int64
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
	job.state = _SucceededJob
}

func (job *_Job) IsSucceeded() bool {
	return job.state == _SucceededJob
}

func (job *_Job) Reject() {
	job.state = _RejectedJob
}

func (job *_Job) IsRejected() bool {
	return job.state == _RejectedJob
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

func (job *_Job) SetMesgID(id int64) {
	job.mesgID = id
}

func (job *_Job) MesgID() int64 {
	return job.mesgID
}

var _uid int64

// NextID returns new id on every call
func NextID() int64 {
	_uid++
	return _uid
}

// NewJob creates new Job object
func NewJob(topic string, payload []byte, priority int, ttl int, qos int) (job Job, err error) {
	if qos < QosRelax || qos > QosComplete {
		glog.Errorf("invalid Job QoS value %d", qos)
		err = errors.New("invalid Job QoS value")
		return
	}
	if priority < PriorityHigh || priority > PriorityLow {
		glog.Errorf("invalid Job priority %d", priority)
		err = errors.New("invalid Job priority")
		return
	}
	job = &_Job{
		id:       NextID(),
		topic:    topic,
		payload:  payload,
		priority: priority,
		ttl:      ttl,
		qos:      qos,
		state:    _UnknownJobState,
	}
	return
}
