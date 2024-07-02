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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

class UDFKeyAddOne:
    def __init__(self):
        pass

    # key add 1, only for test
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        rows = [data[2][0] + 1, data[2][1]]
        res.append(rows)
        return res

    def buildHeader(self, data):
        colNames = ["key"]
        for name in data[0][1:]:
            colNames.append("key_add_one(" + name + ")")
        return [colNames, data[1]]