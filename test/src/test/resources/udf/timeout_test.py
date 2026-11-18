import os
import time
import threading

class TimeoutTest:
    def __init__(self):
        pass

    def timeout(self, data, args, kvargs):
        """dead loop"""
        try:
            limit = 6  # in case of bug
            while limit > 0:
                print('running timeout')
                time.sleep(10)
                limit-=1
        finally:
            pass

    def waitForEvent(self, data, args, kvargs):
        event = threading.Event()
        event.wait()

    def downloadLargeModel(self, data, args, kvargs):
        """download big ML models to test timeout when downloading"""
        print("start to get models...")
        # Do not import torch (e.g. via transformers) here: a timeout that interrupts the import
        # leaves torch half-initialized and breaks every later "import torch" in this process.
        # hf_xet downloads in a single native call, which the timeout cannot interrupt.
        os.environ["HF_HUB_DISABLE_XET"] = "1"
        from huggingface_hub import hf_hub_download
        hf_hub_download("Salesforce/blip-image-captioning-base", "pytorch_model.bin", force_download=True)
        print("finished downloading.")

    def transform(self, data, args, kvargs):
        """测三种情况"""
        if args[0]==1:
            self.timeout(data, args, kvargs)
        elif args[0]==2:
            self.waitForEvent(data, args, kvargs)
        else:
            self.downloadLargeModel(data, args, kvargs)