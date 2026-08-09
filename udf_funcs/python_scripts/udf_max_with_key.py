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
class UDFMaxWithKey:
    def __init__(self):
        pass

    # only take one column, return max value and its key
    def eval(self, data, args, kvargs):
        res = self.buildHeader(data)

        max = None
        maxKey = None
        for row in data[2:]:
            num = row[1]
            key = row[0]
            if num is not None:
                if max is None:
                    max = num
                    maxKey = key
                elif max < num:
                    max = num
                    maxKey = key
        res.append([maxKey, max])
        return res

    def buildHeader(self, data):
        colNames = ["key"]
        for name in data[0][1:]:
            colNames.append("udf_max_with_key(" + name + ")")
        return [colNames, data[1]]