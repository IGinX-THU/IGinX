class ClassA:
    def print_self(self):
        return "class A"

    def print_inner(self):
        from .sub_module.sub_class_a import SubClassA
        obj = SubClassA()
        return obj.print_self()

    def transform(self, data, args, kvargs):
        self.print_self()
        self.print_inner()
        return [["col_outer_a"], ["LONG"], [1]]


class ClassB:
    def print_self(self):
        return "class B"

    def print_inner(self):
        from .sub_module.sub_class_a import SubClassA
        obj = SubClassA()
        return obj.print_self()

    def transform(self, data, args, kvargs):
        self.print_self()
        self.print_inner()
        return [["col_outer_b"], ["LONG"], [1]]
