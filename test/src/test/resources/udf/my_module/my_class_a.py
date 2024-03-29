class ClassA:
    def print_self(self):
        print("class A")
        return "class A"

    def print_inner(self):
        from .sub_module.sub_class_a import SubClassA
        obj = SubClassA()
        return obj.print_self()
