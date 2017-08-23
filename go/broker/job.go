package broker

import (
	"errors"

	"github.com/golang/glog"
)

const (
	unknownJob = iota
	rejectedJob
	succeededJob
)

type theJob struct {
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

func (job *theJob) ID() int64 {
	return job.id
}

func (job *theJob) Result() []byte {
	return job.result
}

func (job *theJob) SetResult(result []byte) {
	job.result = result
}

func (job *theJob) Success() {
	job.state = succeededJob
}

func (job *theJob) IsSucceeded() bool {
	return job.state == succeededJob
}

func (job *theJob) Reject() {
	job.state = rejectedJob
}

func (job *theJob) IsRejected() bool {
	return job.state == rejectedJob
}

func (job *theJob) Topic() string {
	return job.topic
}

func (job *theJob) Payload() []byte {
	return job.payload
}

func (job *theJob) QoS() int {
	return job.qos
}

func (job *theJob) CanRetryWithTTL() bool {
	if job.ttl > 0 {
		job.ttl--
		return true
	}
	return false
}

func (job *theJob) Priority() int {
	return job.priority
}

func (job *theJob) SetMesgID(id int64) {
	job.mesgID = id
}

func (job *theJob) MesgID() int64 {
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
	job = &theJob{
		id:       NextID(),
		topic:    topic,
		payload:  payload,
		priority: priority,
		ttl:      ttl,
		qos:      qos,
		state:    unknownJob,
	}
	return
}
