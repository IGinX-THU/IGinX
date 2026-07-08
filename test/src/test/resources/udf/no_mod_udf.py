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

from iginx_udf import UDTFWrapper

@UDTFWrapper
class NoModUDF():
    """
    :return: return the exact same table after replacing column names with "no_mod(${COL_NAME})"
    """
    def __init__(self):
        pass

    def eval(self, data, args, kvargs):
        for i, element in enumerate(data[0]):
            if i == 0:
                continue  # skip key
            data[0][i] = f"no_mod({element})"
        return data
