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

from iginx_udf import UDAF

class UDFMaxWithKey(UDAF):
    def init_status(self):
        return [-1, 0]

    def build_header(self, paths, types):
        col = f"{self.udf_name}({paths[0]})"
        type = types[0]
        return ['key', col], ['LONG', type]

    def eval(self, status, data):
        if status[0] == -1:
            status[1] = data
            status[0] = self.get_key()
        else:
            if status[1] < data:
                status[1] = data
                status[0] = self.get_key()
        return status
