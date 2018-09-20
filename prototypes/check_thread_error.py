import time
import threading


def target():
    time.sleep(1)
    assert False


thread = threading.Thread(target=target)
thread.start()

time.sleep(2)
thread.join()
time.sleep(10)
