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


class MarkEmptyTransformer:
    def __init__(self):
        pass

    def transform(self, rows):
        if len(rows) > 1:
            raise ValueError("expected empty input, got %d rows" % (len(rows) - 1))
        header = rows[0]
        marks = sum(1 for name in header if name.startswith("mark"))
        return [header + ["mark%d" % marks]]
