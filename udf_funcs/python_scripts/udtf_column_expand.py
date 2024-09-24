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
from iginx_udf import UDTFinDF


class UDFColumnExpand(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        ori_cols = list(data)
        for i, col_name in enumerate(ori_cols):
            new_pos = 3 * i + 1
            add_column = data[col_name] + 1.5
            mul_column = data[col_name] * 2
            data.rename(columns={col_name: f'{self.udf_name}({col_name})'}, inplace=True)
            data.insert(loc=new_pos, column=f'{self.udf_name}({col_name}+1.5)', value=add_column)
            data.insert(loc=new_pos+1, column=f'{self.udf_name}({col_name}*2)', value=mul_column)
        return data
