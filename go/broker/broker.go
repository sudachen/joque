package broker

/**

The set of public interfaces
   regarding to use dependency inversion.

*/

const (
	// QosRelax means client does not require any acknowledge
	QosRelax = iota
	// QosAck means client requires acknowledge on job enqueue
	QosAck
	// QosComplete means client requres acknowledge on job enqueue and on job complete
	QosComplete
)

const (
	// PriorityHigh is the highst job priority
	PriorityHigh = iota
	// PriorityNormal is the normal job priority
	PriorityNormal
	// PriorityLow is the lowest job priority
	PriorityLow
	// PriorityCount is the count of the available priorities
	PriorityCount
)

// Job is the abstraction of a queued job
type Job interface {
	ID() int64
	Result() []byte
	SetResult([]byte)
	Success()
	IsSucceeded() bool
	Reject()
	IsRejected() bool
	Topic() string
	Payload() []byte
	QoS() int
	CanRetryWithTTL() bool
	Priority() int
}

// MesgJob is the abstraction of the binding job to a message id
type MesgJob interface {
	SetMesgID(int64)
	MesgID() int64
}

// Originator is the abstraction of a job originator
type Originator interface {
	ID() int64
	Acknowledge(Job)
	Complete(Job)
}

// Worker is the abstraction of a job execution service
type Worker interface {
	ID() int64
	Execute(Job)
	Disconnect()
}

// Broker is the abstraction of a job queueing service mediating between originators and workers
type Broker interface {
	Enqueue(Job, Originator) (err error)
	Complete(Job, Worker) (err error)
	Subscribe(Worker, string) (err error)
	Unsubscribe(Worker, bool) (err error)
	Stop() (err error)
}
