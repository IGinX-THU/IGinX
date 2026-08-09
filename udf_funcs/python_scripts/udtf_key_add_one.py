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

from iginx_udf import UDTFWrapper

@UDTFWrapper
class UDFKeyAddOne:
    def __init__(self):
        pass

    # key add 1, only for test
    def eval(self, data, args, kvargs):
        res = self.buildHeader(data)
        rows = [data[2][0] + 1, data[2][1]]
        res.append(rows)
        return res

    def buildHeader(self, data):
        colNames = ["key"]
        for name in data[0][1:]:
            colNames.append("key_add_one(" + name + ")")
        return [colNames, data[1]]