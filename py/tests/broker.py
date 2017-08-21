
import sys
import subprocess
import signal
import os.path

ADDRESS = "localhost:9100"

_broker = None

def start(address=ADDRESS):
    global _broker
    if _broker is None:
        executable = os.path.normpath(os.path.dirname(__file__)+"/../../go/joque.exe")
        print(executable)
        _broker = subprocess.Popen(['','--addr='+address],
                               executable = executable,
                               creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)        

def stop():
    global _broker
    if not _broker is None:
        _broker.send_signal(signal.CTRL_BREAK_EVENT)
        _broker.wait()
        _broker = None
