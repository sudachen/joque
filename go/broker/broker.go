package broker

import (
	"errors"
	"time"

	"github.com/golang/glog"
)

// _JoqueBroker is the joque implementation of the broker abstraction
type _JoqueBroker struct {
	chJobEnque    chan _JobEnqueChas
	chJobDone     chan _JobDoneChas
	chSubscribe   chan _SubscribeChas
	chUnsubscribe chan _UnsubscribeChas
	chStop        chan int
	topic         map[string]*_Topic
	ordered       *_Topic
	last          *_Topic
	workers       map[int64]*_WorkerNfo
}

// _JobEnqueChas is an envelope to communicate with broker
//   Broker.Enqueue method uses it to enqueue Job to the broker queue
type _JobEnqueChas struct {
	job  Job
	orig Originator
}

// _JobDoneChas is an envelope to communicate with broker
//   Broker.Complete method uses it to notify broker on job execution end
type _JobDoneChas struct {
	jobID int64
	wrkID int64
}

// _SubscribeChas is an envelope to communicate with broker
//   Broker.Subscribe method use it to subscribe workers to the topic
type _SubscribeChas struct {
	wrk       Worker
	topicName string
}

// _UnsubscribeChas is an envelope to communicate with broker
//   Broker.unsubscribe method use it unsubscribe workers
type _UnsubscribeChas struct {
	wrkID int64
	lost  bool
}

// _Topic is the named queue container
//   workers subscrube to the topic for recivening job to execute
//   originators publish jobs to the topic
type _Topic struct {
	name       string
	wrkQueHead *_WorkerNfo
	wrkQueTail *_WorkerNfo
	jobQueHead *_JobNfo
	jobQueTail [PriorityCount]*_JobNfo
	next       *_Topic
}

// _WorkerNfo is an information about subsrcibed worker
type _WorkerNfo struct {
	wrk     Worker
	topic   *_Topic
	currJob *_JobNfo
	next    *_WorkerNfo
}

// _JobNfo is an information about enqueued job
type _JobNfo struct {
	job     Job
	started time.Time
	orig    Originator
	next    *_JobNfo
}

func (brk *_JoqueBroker) GetTopic(topicName string) (topic *_Topic) {
	topic = brk.topic[topicName]
	if topic == nil {
		topic = &_Topic{name: topicName}
		brk.topic[topicName] = topic
		topic.next = brk.ordered
		brk.ordered = topic
	}
	return
}

func (wrkNfo *_WorkerNfo) Enqueue() {
	wrkID := wrkNfo.wrk.ID()
	if wrkNfo.topic == nil {
		glog.Fatalf("assertion: worker %d is unbond", wrkID)
	}
	topic := wrkNfo.topic
	if wrkNfo.currJob != nil {
		glog.Fatalf("assertion: worker %d has unfinished work and can't be enqueued to the topic %s", wrkID, topic.name)
	}
	if wrkNfo.next != nil || topic.wrkQueTail == wrkNfo {
		glog.Fatalf("assertion: worker %d already enqueued to the topic %s", wrkID, topic.name)
	}
	if topic.wrkQueTail == nil && topic.wrkQueHead != nil {
		glog.Fatal("assertion: topic.wrkQueTail is nil when topic.wrkQueHead is not")
	}
	if topic.wrkQueTail == nil {
		topic.wrkQueHead = wrkNfo
	} else {
		topic.wrkQueTail.next = wrkNfo
	}
	topic.wrkQueTail = wrkNfo
}

func (wrkNfo *_WorkerNfo) Dequeue() {
	topic := wrkNfo.topic
	nfo := topic.wrkQueHead

	if nfo == wrkNfo {
		topic.wrkQueHead = nfo.next
		if topic.wrkQueHead == nil {
			topic.wrkQueTail = nil
		}
		return
	}

	for nfo != nil {
		if nfo.next == wrkNfo {
			nfo.next = wrkNfo.next
			if nfo.next == nil {
				topic.wrkQueTail = nfo
			}
			break
		}
	}

	wrkNfo.next = nil
}

func (brk *_JoqueBroker) RegisterWorker(wrk Worker, topicName string) {
	if brk.workers[wrk.ID()] != nil {
		glog.Errorf("worker %d already registered in topic %s", wrk.ID(), brk.workers[wrk.ID()].topic.name)
		return
	}
	topic := brk.GetTopic(topicName)
	wrkNfo := &_WorkerNfo{wrk: wrk, topic: topic}
	brk.workers[wrk.ID()] = wrkNfo
	wrkNfo.Enqueue()
}

func (brk *_JoqueBroker) UnregisterWorker(wrkID int64, lost bool) {
	wrkNfo := brk.workers[wrkID]
	if wrkNfo == nil {
		glog.Errorf("worker %d is not registered", wrkID)
		return
	}
	wrkNfo.Dequeue()
	if wrkNfo.currJob != nil {
		job := wrkNfo.currJob.job
		orig := wrkNfo.currJob.orig
		if !lost || job.CanRetryWithTTL() {
			brk.EnqueueJob(job, orig)
		} else {
			if job.QoS() >= QosComplete {
				brk.CompleteJob(orig, job)
			}
		}
	}
	delete(brk.workers, wrkID)
}

func (brk *_JoqueBroker) EnqueueJob(job Job, orig Originator) {
	prior := job.Priority()
	if prior < PriorityHigh && prior > PriorityLow {
		glog.Errorf("job %d {%s} has invalid priority %d", job.ID(), job.Topic(), prior)
		return
	}
	topic := brk.GetTopic(job.Topic())
	nfo := &_JobNfo{job: job, orig: orig}
	afprior := prior
	for afprior >= PriorityHigh {
		if topic.jobQueTail[afprior] != nil {
			last := topic.jobQueTail[afprior]
			nfo.next = last.next
			last.next = nfo
			topic.jobQueTail[prior] = nfo
			break
		}
		afprior--
	}
	if afprior < PriorityHigh {
		if topic.jobQueHead != nil && topic.jobQueHead.job.Priority() <= prior {
			glog.Fatalf("assertion: topic.jobQueTail[?<=%d] is nil when topic.jobQueHead is not and has upper or equal priority %d",
				prior, topic.jobQueHead.job.Priority())
		}
		nfo.next = topic.jobQueHead
		topic.jobQueHead = nfo
		topic.jobQueTail[prior] = nfo
	}
	glog.Infof("job %d enqueued", job.ID())
}

func (topic *_Topic) ExecuteNextJob() (wrkNfo *_WorkerNfo) {
	jobNfo := topic.jobQueHead
	if jobNfo == nil || topic.wrkQueHead == nil {
		return
	}
	topic.jobQueHead = jobNfo.next
	jobNfo.next = nil
	prior := jobNfo.job.Priority()
	if topic.jobQueTail[prior] == jobNfo {
		topic.jobQueTail[prior] = nil
	}
	wrkNfo = topic.wrkQueHead
	topic.wrkQueHead = wrkNfo.next
	wrkNfo.next = nil
	if topic.wrkQueHead == nil {
		topic.wrkQueTail = nil
	}
	wrkNfo.currJob = jobNfo
	jobNfo.started = time.Now()
	wrkNfo.wrk.Execute(jobNfo.job)
	return
}

func (brk *_JoqueBroker) ExecuteNextJob() (wrkNfo *_WorkerNfo) {
	topic := brk.last

	if topic == nil {
		if brk.ordered == nil {
			return
		}
		topic = brk.ordered
		brk.last = topic
	} else if topic.next == nil {
		topic = brk.ordered
	}

	for {
		wrkNfo = topic.ExecuteNextJob()
		glog.Infof("topic.ExecuteNextJob() -> %x", wrkNfo)
		if wrkNfo == nil {
			topic = topic.next
			if topic == nil {
				topic = brk.ordered
			}
			if brk.last == topic {
				return
			}
		} else {
			break
		}
	}

	brk.last = topic
	return
}

func (brk *_JoqueBroker) ExecuteJobs() {
	for {
		if brk.ExecuteNextJob() == nil {
			break
		}
	}
}

func (brk *_JoqueBroker) JobDone(wrkID int64, jobID int64) (job Job, orig Originator) {
	wrk := brk.workers[wrkID]
	if wrk == nil {
		glog.Errorf("opss, there is no worker with id %d which just done job %d", wrkID, jobID)
		return
	}

	currJob := wrk.currJob
	job = currJob.job

	if job.ID() != jobID {
		glog.Errorf("opss, worker with id %d which just done job %d executes job %d", wrkID, jobID, job.ID())
		return
	}

	wrk.currJob = nil
	wrk.Enqueue()

	if !job.IsSucceeded() {
		if job.CanRetryWithTTL() {
			brk.EnqueueJob(job, currJob.orig)
			job = nil
			return
		}
	}

	orig = currJob.orig
	return
}

func (brk *_JoqueBroker) CompleteJob(orig Originator, job Job) {
	// have i use a goroutine?!
	orig.Complete(job)
}

func (brk *_JoqueBroker) AcknowledgeJob(orig Originator, job Job) {
	// have i use a goroutine?!
	orig.Acknowledge(job)
}

// StartJoqueBroker starts joque broker
func StartJoqueBroker() Broker {

	brk := &_JoqueBroker{
		topic:         make(map[string]*_Topic),
		workers:       make(map[int64]*_WorkerNfo),
		chJobDone:     make(chan _JobDoneChas),
		chJobEnque:    make(chan _JobEnqueChas),
		chSubscribe:   make(chan _SubscribeChas),
		chUnsubscribe: make(chan _UnsubscribeChas),
		chStop:        make(chan int),
	}

	c := make(chan struct{})

	go func() {
		glog.Infof("joque broker started")
		close(c)

		for {
			select {
			case <-brk.chStop:
				glog.Infof("stopping joque broker")

				close(brk.chJobDone)
				close(brk.chJobEnque)
				close(brk.chSubscribe)
				close(brk.chUnsubscribe)

				for _, wrkNfo := range brk.workers {
					glog.Infof("%v", wrkNfo)
					wrkNfo.wrk.Disconnect()
				}

				brk.workers = nil
				brk.ordered = nil
				brk.last = nil
				brk.topic = nil

				glog.Infof("joque broker stoped")
				close(brk.chStop)
				return
			case chas := <-brk.chJobDone:
				glog.Infof("done job %d", chas.jobID)
				job, orig := brk.JobDone(chas.wrkID, chas.jobID)
				if job != nil && job.QoS() >= QosComplete {
					brk.CompleteJob(orig, job)
				}
				brk.ExecuteJobs()
			case chas := <-brk.chJobEnque:
				glog.Infof("enqueue job %d", chas.job.ID())
				brk.EnqueueJob(chas.job, chas.orig)
				if chas.job.QoS() > QosRelax {
					brk.AcknowledgeJob(chas.orig, chas.job)
				}
				brk.ExecuteJobs()
			case chas := <-brk.chSubscribe:
				brk.RegisterWorker(chas.wrk, chas.topicName)
				brk.ExecuteJobs()
			case chas := <-brk.chUnsubscribe:
				brk.UnregisterWorker(chas.wrkID, chas.lost)
			}
		}
	}()

	_ = <-c
	return brk
}

/**

Broker interface implementation

*/

func (brk *_JoqueBroker) Enqueue(job Job, orig Originator) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chJobEnque <- _JobEnqueChas{job, orig}
	return
}

func (brk *_JoqueBroker) Complete(job Job, wrk Worker) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chJobDone <- _JobDoneChas{job.ID(), wrk.ID()}
	return
}

func (brk *_JoqueBroker) Subscribe(wrk Worker, topic string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chSubscribe <- _SubscribeChas{wrk, topic}
	return
}

func (brk *_JoqueBroker) Unsubscribe(wrk Worker, lost bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chUnsubscribe <- _UnsubscribeChas{wrk.ID(), lost}
	return
}

func (brk *_JoqueBroker) Stop() (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()

	brk.chStop <- 0
	_ = <-brk.chStop

	return
}
