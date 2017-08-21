
import sys
import subprocess
import signal
import os.path
import platform
import time

ADDRESS = "localhost:9100"

_broker = None

SUBPROCESS_FLAGS = 0
if platform.system() == 'Windows':
    SUBPROCESS_FLAGS  =subprocess.CREATE_NEW_PROCESS_GROUP
    SUBPROCESS_SIGNAL = signal.SIGBREAK
    SUBPROCESS_EVENT  = signal.CTRL_BREAK_EVENT
else:
    SUBPROCESS_SIGNAL = signal.SIGTERM
    SUBPROCESS_EVENT  = signal.SIGTERM
    
def start(address=ADDRESS):
    global _broker
    if _broker is None:
        executable = os.path.normpath(os.path.dirname(__file__)+"/../../go/joque.exe")
        print(executable)
        _broker = subprocess.Popen(['','-addr='+address],
                               executable = executable,
                               creationflags=SUBPROCESS_FLAGS)  
        time.sleep(0.5)      

def stop():
    global _broker
    if not _broker is None:
        _broker.send_signal(SUBPROCESS_EVENT)
        _broker.wait()
        _broker = None
