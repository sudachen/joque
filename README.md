## Terms

__Joque__ - the job queueing service mediating between job originators and workers.   
   The queueing broker is coded in Go. Client API has Python interface.
   
__Originator__ - the source of job instructions

__Worker__ - the service subscribed on topic and executes job instructions

__Job__ - some instructions to do job.    
   The broker has no business with instructions, only with jobs queueing, passing them to workers and returnig result to originators, if it is required.

__Topic__ - the rendezvous point where meet originator, worker and job queue.

__Queue__ - the queue of jobs. 

__QoS__ - the quality of service level.   
   Every job declares required QoS. There are three levels: QosRelax, QosAck, QosComplete. QosRelax means originator does not warry about acknoladgments. It does likes fire and forget. QosAck - Originator requires acknoladgment when job enqueueued. QosComplete - originator requires job result or acknoladgment when jobe is done.
   
__Priority__  - the job priority.   
   Every job declares self priority . There are three levels: PriorityHigh, PriorityNormal,PriorityLow.
   
__TTL__  - the time to live.   
   Every job declares how many times it can be reenqueued if worker failed to execute it.
   
## The main actors and their connections

![](joque_1.jpg)
   
## Broker (Golang)

The broker divided into three packages - broker, server, transport. 

* The __broker package__ contains broker interfaces and function StartJoqueBroker.
* The __transport package__ contains simple ASCII messaging transport ASCIIMqt and function Upgrade.
* The __server package__ contains functions Connect and StartJoqueServer

## API (Python)

There is only one module joque.py 

The worker code looks like

```python
from joque import Joque

jq = Joque('topic1')

@jq.Job(QoS=jq.QosRelax)
def hello(text):
    print(text)


@jq.Job()
def upper(text):
    return str(text).upper()


@jq.Job()
def rise_expt(text):
    raise Exception(text)

def serve(*args, **kw):
    jq.start(__file__, *args, **kw)


if __name__ == '__main__':
    jq.serve()
```

Jobs can be used in following maner

```python
from joque import Joque

Joque.set_broker("localhost:9100")

# workers can be started just here or on other pc
topic1.serve()

topic1.hello("hello world!")
s = topic1.upper("text").result()
```

