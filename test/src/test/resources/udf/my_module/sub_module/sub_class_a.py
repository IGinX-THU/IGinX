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

from iginx_udf import UDSFWrapper

@UDSFWrapper
class SubClassA:
    def print_self(self):
        return "sub class A"

    def print_outer(self):
        from ..my_class_a import ClassA
        obj = ClassA()
        return obj.print_self()

    def eval(self, data, args, kvargs):
        self.print_self()
        self.print_outer()
        return [["col_inner"], ["LONG"], [1]]
