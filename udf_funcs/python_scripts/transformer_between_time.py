#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

"""
Select values between particular times of the day (e.g., 9:00-9:30 AM).
"""
import pandas as pd


class MyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        df = pd.DataFrame(rows)
        df = df.between_time(start_time, end_time, include_start=NoDefault.no_default,
                               include_end=NoDefault.no_default, inclusive=None,
                               axis=None).values.tolist()  # (start_time, end_time, include_start=NoDefault.no_default,
        # include_end=NoDefault.no_default, inclusive=None, axis=None)
        ret = df.values.tolist()
        ret.insert(0, df.keys().values.tolist())
        return ret
