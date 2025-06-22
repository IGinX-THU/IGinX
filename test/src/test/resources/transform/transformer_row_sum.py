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
import numpy as np


class RowSumTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows[1:], columns=rows[0])
        ret = np.zeros((df.shape[0], 2), dtype=np.int32)
        for index, row in df.iterrows():
            row_sum = 0
            for num in row[1:]:
                row_sum += num
            ret[index][0] = row.iloc[0]
            ret[index][1] = row_sum

        df = pd.DataFrame(ret, columns=['key', 'sum'])
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
