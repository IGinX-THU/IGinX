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
Sorts index in descending order:

DataFrame.sort_index(axis=0, level=None, ascending=True, inplace=False, kind='quicksort', na_position='last',
sort_remaining=True, ignore_index=False, key=None
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        # By default, it sorts in ascending order, to sort in descending order, use ascending=False
        df = df.sort_index(axis=0, level=None, ascending=False, inplace=False, kind='quicksort', na_position='last', sort_remaining=True, ignore_index=False, key=None)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
