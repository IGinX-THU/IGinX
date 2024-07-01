#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
 
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
