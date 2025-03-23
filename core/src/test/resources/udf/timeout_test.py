import time
import threading

class TimeoutTest:
    def __init__(self):
        pass

    def timeout(self, data, args, kvargs):
        """dead loop"""
        try:
            limit = 6  # in case of bug
            while limit>=0:
                print('running timeout')
                time.sleep(10)
                limit-=1
        finally:
            pass

    def waitForEvent(self, data, args, kvargs):
        event = threading.Event()
        event.wait()