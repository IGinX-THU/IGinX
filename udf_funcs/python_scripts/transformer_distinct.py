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
Writing a distinct() function for IginX:
Count the distinct field values associated with a field key.
1. Return DataFrame with duplicate rows removed.
2. To remove duplicates on specific column(s), use subset.
"""
import pandas as pd


class MyTransformer:  # a class is an object constructor, or a "blueprint" for creating objects
    # All classes have a function called __init__(), it is called automatically every time the class is being used to
    # create a new object.
    def __init__(self):  # the self parameter is a reference to the current instance of the class, used to access
        # variables that belong to the class distinctTransformer()
        pass

    def transform(self, rows):
        # dropping all duplicate values
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=df.columns.values[1:])
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
