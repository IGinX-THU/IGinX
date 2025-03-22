import time


class TimeoutTest:
    def __init__(self):
        pass

    def timeout(self, data, args, kvargs):
        """dead loop"""
        try:
            while True:
                print('running timeout')
                time.sleep(10)
        finally:
            pass

    def downloadLargeModel(self, data, args, kvargs):
        """download big ML models to test timeout when downloading"""
        print("start to get models...")
        from transformers import BlipProcessor, BlipForConditionalGeneration
        processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
        model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
        print("finished downloading.")