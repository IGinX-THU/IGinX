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
 
from dateutil import parser


class Test:
    def transform(self, data, args, kvargs):
        datetime_string = "2023-04-05 14:30:00"
        parsed_datetime = parser.parse(datetime_string)

        # 验证解析结果
        if parsed_datetime.year == 2023 and \
                parsed_datetime.month == 4 and \
                parsed_datetime.day == 5 and \
                parsed_datetime.hour == 14 and \
                parsed_datetime.minute == 30:
            return [
                ["year", "month", "day", "hour", "minute"],
                ["LONG", "LONG", "LONG", "LONG", "LONG"],
                [parsed_datetime.year, parsed_datetime.month, parsed_datetime.day, parsed_datetime.hour, parsed_datetime.minute]
            ]
        else:
            return []

