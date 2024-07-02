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

class UDFMax:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)

        maxRow = []
        rows = data[2:]
        for row in list(zip(*rows))[1:]:
            max = None
            for num in row:
                if num is not None:
                    if max is None:
                        max = num
                    elif max < num:
                        max = num
            maxRow.append(max)
        res.append(maxRow)
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0][1:]:
            colNames.append("udf_max(" + name + ")")
        return [colNames, data[1][1:]]
