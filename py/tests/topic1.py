
import env
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
