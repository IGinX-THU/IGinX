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

class UDFColumnExpand:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        newRow = []
        for num in data[2][1:]:
            newRow.append(num)
            newRow.append(num + 1.5)
            newRow.append(num * 2)
        res.append(newRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for i in range(1, len(data[0])):
            colNames.append("column_expand(" + data[0][i] + ")")
            colTypes.append(data[1][i])
            colNames.append("column_expand(" + data[0][i] + "+1.5)")
            colTypes.append("DOUBLE")
            colNames.append("column_expand(" + data[0][i] + "*2)")
            colTypes.append(data[1][i])

        return [colNames, colTypes]
