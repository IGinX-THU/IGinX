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
mean() returns the average of non-null values in a specified column from each input table.
Mean function returns the average by dividing the sum of the values in the set by their number.
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        # Average of each column using DataFrame.mean()
        df = pd.DataFrame(rows)
        df = df.mean(axis=0)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
