#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
from iginx_udf import UDAFWrapper

@UDAFWrapper
class UDFAvg:
    def __init__(self):
        pass

    def eval(self, data, args, kvargs):
        res = self.buildHeader(data)

        avgRow = []
        rows = data[2:]
        for row in list(zip(*rows))[1:]:
            sum, count = 0, 0
            for num in row:
                if num is not None:
                    sum += num
                    count += 1
            avgRow.append(sum / count)
        res.append(avgRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0][1:]:
            colNames.append("udf_avg(" + name + ")")
            colTypes.append("DOUBLE")
        return [colNames, colTypes]
