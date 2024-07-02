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

class UDFPow:
  def __init__(self):
    self._n = 1
    pass

  def transform(self, data, args, kvargs):
    n = self._n
    if 'n' in kvargs:
      n = kvargs['n']
    elif len(args) == 1:
      n = args[0]

    res = self.buildHeader(data, n)
    cosRow = []
    for num in data[2][1:]:
      cosRow.append(float(num ** n))
    res.append(cosRow)
    return res

  def buildHeader(self, data, n):
    colNames = []
    colTypes = []
    for name in data[0][1:]:
      colNames.append("pow({col}, {n})".format(col=name, n=n))
      colTypes.append("DOUBLE")
    return [colNames, colTypes]
