class ClassA:
    def transform(self, data, args, kvargs):
        return [["col_outer_a"], ["LONG"], [1]]


class ClassB:
    def transform(self, data, args, kvargs):
        return [["col_outer_b"], ["LONG"], [1]]


class ClassC:
    def transform(self, data, args, kvargs):
        return [["col_outer_c"], ["LONG"], [1]]
