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


class EmptyTransformer:
    def transform(self, rows):
        return [rows[0]]


class NoneTransformer:
    def transform(self, rows):
        return None


class ImplicitNoneTransformer:
    def transform(self, rows):
        rows[0]


class NonTwoDimensionalTransformer:
    def transform(self, rows):
        return tuple(rows[0])


class ColumnCountMismatchTransformer:
    def transform(self, rows):
        return [["key", "value"], [1]]


class InvalidValueTypeTransformer:
    def transform(self, rows):
        return [["key", "value"], [1, {"value": 1}]]


class InconsistentColumnTypeTransformer:
    def transform(self, rows):
        return [["key", "value"], [1, 1], [2, "value"]]
