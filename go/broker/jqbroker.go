package broker

import (
	"errors"
	"math"
	"time"

	"github.com/golang/glog"
)

// theJoqueBroker is the joque implementation of the broker abstraction
type theJoqueBroker struct {
	chJobEnque    chan jobEnqueChas
	chJobDone     chan jobDoneChas
	chSubscribe   chan subscribeChas
	chUnsubscribe chan unsubscribeChas
	chStop        chan int
	topic         map[string]*theTopic
	ordered       *theTopic
	last          *theTopic
	workers       map[int64]*theWorkerNfo
	maxQueLength  int
	curQueLength  int
}

// jobEnqueChas is an envelope to communicate with broker
//   Broker.Enqueue method uses it to enqueue Job to the broker queue
type jobEnqueChas struct {
	job  Job
	orig Originator
}

// jobDoneChas is an envelope to communicate with broker
//   Broker.Complete method uses it to notify broker on job execution end
type jobDoneChas struct {
	jobID int64
	wrkID int64
}

// subscribeChas is an envelope to communicate with broker
//   Broker.Subscribe method use it to subscribe workers to the topic
type subscribeChas struct {
	wrk       Worker
	topicName string
}

// unsubscribeChas is an envelope to communicate with broker
//   Broker.unsubscribe method use it unsubscribe workers
type unsubscribeChas struct {
	wrkID int64
	lost  bool
}

// theTopic is the named queue container
//   workers subscrube to the topic for recivening job to execute
//   originators publish jobs to the topic
type theTopic struct {
	name       string
	wrkQueHead *theWorkerNfo
	wrkQueTail *theWorkerNfo
	jobQueHead *theJobNfo
	jobQueTail [PriorityCount]*theJobNfo
	next       *theTopic
}

// theWorkerNfo is an information about subsrcibed worker
type theWorkerNfo struct {
	wrk     Worker
	topic   *theTopic
	currJob *theJobNfo
	next    *theWorkerNfo
}

// theJobNfo is an information about enqueued job
type theJobNfo struct {
	job     Job
	started time.Time
	orig    Originator
	next    *theJobNfo
}

func (brk *theJoqueBroker) GetTopic(topicName string) (topic *theTopic) {
	topic = brk.topic[topicName]
	if topic == nil {
		topic = &theTopic{name: topicName}
		brk.topic[topicName] = topic
		topic.next = brk.ordered
		brk.ordered = topic
	}
	return
}

func (wrkNfo *theWorkerNfo) Enqueue() {
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

func (wrkNfo *theWorkerNfo) Dequeue() {
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

func (brk *theJoqueBroker) RegisterWorker(wrk Worker, topicName string) {
	glog.Infof("worker %d sregistered in topic %s", wrk.ID(), topicName)
	if brk.workers[wrk.ID()] != nil {
		glog.Errorf("worker %d already registered in topic %s", wrk.ID(), brk.workers[wrk.ID()].topic.name)
		return
	}
	topic := brk.GetTopic(topicName)
	wrkNfo := &theWorkerNfo{wrk: wrk, topic: topic}
	brk.workers[wrk.ID()] = wrkNfo
	wrkNfo.Enqueue()
}

func (brk *theJoqueBroker) UnregisterWorker(wrkID int64, lost bool) {
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

func (brk *theJoqueBroker) EnqueueJob(job Job, orig Originator) {
	prior := job.Priority()
	if prior < PriorityHigh && prior > PriorityLow {
		glog.Errorf("job %d {%s} has invalid priority %d", job.ID(), job.Topic(), prior)
		return
	}
	topic := brk.GetTopic(job.Topic())
	nfo := &theJobNfo{job: job, orig: orig}
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

func (topic *theTopic) ExecuteNextJob() (wrkNfo *theWorkerNfo) {

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

func (brk *theJoqueBroker) ExecuteNextJob() (wrkNfo *theWorkerNfo) {
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

func (brk *theJoqueBroker) ExecuteJobs() {
	for {
		if brk.ExecuteNextJob() == nil {
			break
		}
	}
}

func (brk *theJoqueBroker) JobDone(wrkID int64, jobID int64) (job Job, orig Originator) {
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

func (brk *theJoqueBroker) CompleteJob(orig Originator, job Job) {
	// have i use a goroutine?!
	orig.Complete(job)
}

func (brk *theJoqueBroker) AcknowledgeJob(orig Originator, job Job) {
	// have i use a goroutine?!
	orig.Acknowledge(job)
}

// StartJoqueBroker starts joque broker
func StartJoqueBroker(maxQueLength int) Broker {

	if maxQueLength <= 0 {
		maxQueLength = math.MaxInt32
	}

	brk := &theJoqueBroker{
		topic:         make(map[string]*theTopic),
		workers:       make(map[int64]*theWorkerNfo),
		chJobDone:     make(chan jobDoneChas),
		chJobEnque:    make(chan jobEnqueChas),
		chSubscribe:   make(chan subscribeChas),
		chUnsubscribe: make(chan unsubscribeChas),
		chStop:        make(chan int), // unbuffered!
		maxQueLength:  maxQueLength,
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

	// waiting until coroutine be started successful
	<-c
	return brk
}

/**

Broker interface implementation

*/

func (brk *theJoqueBroker) Enqueue(job Job, orig Originator) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chJobEnque <- jobEnqueChas{job, orig}
	return
}

func (brk *theJoqueBroker) Complete(job Job, wrk Worker) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chJobDone <- jobDoneChas{job.ID(), wrk.ID()}
	return
}

func (brk *theJoqueBroker) Subscribe(wrk Worker, topic string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chSubscribe <- subscribeChas{wrk, topic}
	return
}

func (brk *theJoqueBroker) Unsubscribe(wrk Worker, lost bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()
	brk.chUnsubscribe <- unsubscribeChas{wrk.ID(), lost}
	return
}

func (brk *theJoqueBroker) Stop() (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = errors.New("broker is stopped")
		}
	}()

	brk.chStop <- 0 // signal goroutine to stop
	<-brk.chStop    // wating until goroutine be stoped

	return
}
