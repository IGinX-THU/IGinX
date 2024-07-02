class MockUDF():
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = [["col"], ["LONG"], [1]]
        return res
