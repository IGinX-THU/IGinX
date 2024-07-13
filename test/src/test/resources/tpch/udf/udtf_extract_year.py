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

from datetime import datetime, timezone
import pytz
class UDFExtractYear:
    def __init__(self):
        pass

    def extractYear(self, num):
        # Unix timestamp is in milliseconds
        timestamp_in_seconds = num / 1000
        # TODO 直接将timestamp增加8小时
        tz = pytz.timezone('Asia/Shanghai')
        dt = datetime.fromtimestamp(timestamp_in_seconds, tz=tz)
        return float(dt.year)
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        dateRow = []
        for num in data[2][1:]:
            dateRow.append(self.extractYear(num))
        res.append(dateRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0][1:]:
            colNames.append("extractYear(" + name + ")")
            colTypes.append("DOUBLE")
        return [colNames, colTypes]
