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
import pandas as pd

from iginx_udf import UDAFinDF


class UDFMin(UDAFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        columns = list(data)
        res = {}
        udf_name = self.udf_name
        for col_name in columns:
            col = data[col_name]
            min_val = col.min()
            res[f"{udf_name}({col_name})"] = [min_val]
        return pd.DataFrame(data=res)
