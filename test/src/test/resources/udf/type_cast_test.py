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

class TypeCastTest():
    """
    测试用的UDF
    """
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        values = [
            [1, 23372, 567, 1, 9999],
            [0.5, 2.71828, 9.876, 2.5, 3.1415926535],
            [True, False, True, False, True],
            ["b", "-453625", "5.327", "false", "aaa"]
        ]
        res.extend(values)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = ["INTEGER", "LONG", "DOUBLE", "BOOLEAN", "BINARY"]
        for colType in colTypes:
            colNames.append("typeCastTest(us.d1." + colType + ")")
        return [colNames, colTypes]
