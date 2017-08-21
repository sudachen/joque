
import inspect
import marshal
import sys
import subprocess
import signal
import os
import socket
import logging
import codecs
import time
import re
import platform

SUBPROCESS_FLAGS = 0
if platform.system() == 'Windows':
    SUBPROCESS_FLAGS  = subprocess.CREATE_NEW_PROCESS_GROUP
    SUBPROCESS_SIGNAL = signal.SIGBREAK
    SUBPROCESS_EVENT  = signal.CTRL_BREAK_EVENT
else:
    SUBPROCESS_SIGNAL = signal.SIGTERM
    SUBPROCESS_EVENT  = signal.SIGTERM
    
class Constants:
    QosRelax = 0
    QosAck = 1
    QosComplete = 2

    PriorityHigh = 0
    PriorityNormal = 1
    PriorityLow = 2

    DefaultTTL = 1


_next_id = 0
def next_id():
    global _next_id
    _next_id += 1
    return _next_id

class Connect:
    
    def close(self):
        if not self.s is None:
            try:
                self.send_line(b'0 QUIT')
                self.send_line()
            except:
                pass
            self.s.close()
            self.s = None

    def __del__(self):
        self.close()

    def __init__(self,host,port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host,port))
        self.s.settimeout(0.5)

    def send_line(self,s=None):
        if s:
            self.s.send(s)
        self.s.send(b"\n")

    def recv_line(self):
        b = []
        while True:
            try:
                d = self.s.recv(1)
                if len(d) > 0 :
                    if d[0] != ord('\n'):
                        b.append(d[0])
                    else:
                        return bytes(b)
            except socket.timeout:
                pass

    def connect(self):
        self.send_line(b"0 CONNECT")
        self.send_line()
        self.expect_ack(0)

    def expect_ack(self,id):
        f = "{} ACK".format(id).encode()
        s = self.recv_line()
        if s != f:
            raise Exception("excpected {} recv {}".format(f,s))
        s = self.recv_line()
        if len(s) != 0:
            if s == b'REJECTED':
                raise Exception("rejected")
            self.recv_line()

    def query(self,id):
        s = "{} QUERY".format(id).encode()
        self.send_line(s)
        self.send_line()
        f1 = "{} ACK".format(id).encode()
        f2 = "{} COMPLETE".format(id).encode()
        s = self.recv_line()
        if s != f1 and s != f2:
            raise Exception("excpected {} of {} recv {}".format(f1,f2,s))
        payload = self.recv_line()
        if len(payload) != 0:
            self.recv_line()
        return ("ACK" if s == f1 else "COMPLETE",payload)

    def publish(self,id,topic,ttl,qos,priority,payload):
        s = "{} PUBLISH {} {} {} {}".format(id,topic,qos,priority,ttl).encode()
        self.send_line(s)
        if payload:
            self.send_line(payload)
        self.send_line()
        if qos >= Constants.QosAck:
            self.expect_ack(id)

    def subscribe(self,id,topic):
        s = "{} SUBSCRIBE {}".format(id,topic).encode()
        self.send_line(s)
        self.send_line()
        self.expect_ack(id)

    rxPUBLISH = re.compile(r'(\d+) PUBLISH (\S+) (\d+) (\d+) (\d+)')

    def get_job(self):
        s = self.recv_line()
        payload = self.recv_line()
        if len(payload) != 0:
            self.recv_line()
        m = self.rxPUBLISH.match(s.decode())
        if m:
            return (m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),payload)        
        return None

    def ack(self,id,reject=False):
        s = "{} ACK".format(id).encode()
        self.send_line(s)
        if reject:
            self.send_line(b'REJECT')
        self.send_line()
        
    def complete(self,id,payload):
        s = "{} COMPLETE".format(id).encode()
        self.send_line(s)
        if payload:
            self.send_line(payload)
        self.send_line()

class Joque(Constants):
    "the main interface to jokue API"

    def _connect(self):
        addr = os.getenv("JOQUE_BROKER_ADDRESS","localhost:2016")
        host,port = addr.split(':')
        port = int(port)
        self.conn = Connect(host,port)
        self.conn.connect()

    def __del__(self):
        if not self.conn is None:
            self.conn.close()
            self.conn = None

    def _marshal(self,tup):
        return (codecs.escape_encode(marshal.dumps(tup)))[0]

    def _unmarshal(self,b):
        return marshal.loads(codecs.escape_decode(b)[0])

    def _remote(self,job,varg):
        job_id = next_id()
        name = job.name
        payload = self._marshal((name,varg))
        if self.conn is None:
            self._connect()
        self.conn.publish(job_id,self.topic,job.ttl,job.qos,job.priority,payload)
        if job.qos >= self.QosComplete:
            return job.Result(job_id)

    def _register(self,name,job):
        self.jobs[name] = job

    class _Job:
        "the job decorator"

        class _Result:
            def __init__(self,job,job_id):
                self._result = None
                self._error  = None
                self._completed = False
                self._job_id = job_id
                self._job = job
            
            def result(self):
                while not self._completed:
                    self.try_complete()
                    time.sleep(0.05)                    
                if not self._error is None:
                    raise Exception(self._error)
                return self._result

            def try_complete(self):
                what, payload = self._job.jq.conn.query(self._job_id)
                if what == "COMPLETE":
                    self._completed = True
                if self._completed and not payload is None:
                    if len(payload):
                        self._result, self._error = self._job.jq._unmarshal(payload)
                    else:
                        payload = None          

            def completed(self):
                if not self._completed:
                    self.try_complete()
                return self._completed

            def succeeded(self):
                return self._completed and self._error is None

        def Result(self,job_id):
            return self.__class__._Result(self,job_id)

        class _Proxy:
            "the job proxy"

            def __init__(self,job,f):                
                self.job = job
                job.func = f
                job.sign = inspect.signature(f)
                job.name = getattr(f,'__name__')
                job.jq._register(job.name,job)

            def __call__(self,*a):
                if len(a) != len(self.job.sign.parameters):
                    raise TypeError("incorrect number of parameters")
                return self.job.jq._remote(self.job,a)

        def __init__(self, jq,
                        QoS=Constants.QosComplete,
                        TTL=Constants.DefaultTTL,
                        Priority=Constants.PriorityNormal):
            self.jq = jq
            self.qos = QoS
            self.ttl = TTL
            self.priority = Priority
            self.func = None
            self.name = None
            self.sign = None

        def __call__(self, f):
            return self.__class__._Proxy(self,f)
        
        def call(self, *a):
            return self.func(*a)
            

    def Job(self,*a,**kw) :
        return self.__class__._Job(self,*a,**kw)

    def __init__(self, topic):
        self.topic = topic
        self.conn = None
        self.jobs = {}

    @staticmethod
    def set_broker(broker_addr):
        "sets broker host:port"
        os.environ["JOQUE_BROKER_ADDRESS"] = broker_addr
        pass

    workers = []

    @staticmethod
    def shutdown():
        "graceful shutdown for all shadow processes"
        for p in Joque.workers:
            p.send_signal(SUBPROCESS_EVENT)
            p.wait()

    def start(self, topic_file, workers_cont=1):
        "start required shutdown processes"
        
        for i in range(workers_cont) :
            Joque.workers.append(self.start_worker(topic_file))

    def start_worker(self, topic_file):
        "starts one worker"
        return subprocess.Popen(['',topic_file],
                               executable = sys.executable,
                               stdin=subprocess.PIPE,
                               creationflags=SUBPROCESS_FLAGS)

    def serve(self):
        "serve for jobs as a worker"

        def sig_handle(signum, frame):
            raise KeyboardInterrupt()
        signal.signal(SUBPROCESS_SIGNAL, sig_handle)
        
        try:
            if self.conn is None:
                self._connect()

            self.conn.subscribe(next_id(),self.topic)

            while True:
                id, topic, qos, prior, ttl, payload = self.conn.get_job()
                result = None
                error = None
                
                try:
                    job_name, args = self._unmarshal(payload)
                    job = self.jobs.get(job_name)
                    if job:
                        result = job.call(*args)
                    else:
                        raise Exception("unknown job " + job_name)
                except Exception as e:
                    error = str(e)
                
                payload = self._marshal((result,error))
                
                self.conn.complete(id,payload)

        except KeyboardInterrupt:    
            pass

        self.conn.close()
