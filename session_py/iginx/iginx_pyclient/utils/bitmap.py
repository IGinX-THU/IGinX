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

class Bitmap(object):
    BIT_UTIL = [1, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7]

    def __init__(self, size, bits=None):
        self.__size = size
        if bits is None:
            self.bits = []
            for i in range(size // 8 + 1):
                self.bits.append(0)
        else:
            self.bits = bits


    def set(self, position):
        self.bits[position // 8] |= Bitmap.BIT_UTIL[position % 8]


    def get(self, position):
        return (self.bits[position // 8] & Bitmap.BIT_UTIL[position % 8]) == Bitmap.BIT_UTIL[position % 8]


    def get_bytes(self):
        return self.bits