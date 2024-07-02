class SubClassA:
    def print_self(self):
        return "sub class A"

    def print_outer(self):
        from ..my_class_a import ClassA
        obj = ClassA()
        return obj.print_self()

    def transform(self, data, args, kvargs):
        self.print_self()
        self.print_outer()
        return [["col_inner"], ["LONG"], [1]]
