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

"""
The bottom() function sorts a table by columns and keeps only the bottom n records. bottom() is a selector function.
bottom() 函数按列对表进行排序并仅保留底部的 n 条记录。 bottom() 是一个选择器函数。

Returns the smallest N field values associated with the field key.
返回与字段键关联的最小 N 字段值。
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows, n=1):
        # Return the last n rows from df
        df = pd.DataFrame(rows)
        df = df.tail(n)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
