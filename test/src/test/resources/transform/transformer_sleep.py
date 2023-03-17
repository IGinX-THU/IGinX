import time


class SleepTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        while True:
            time.sleep(60)
