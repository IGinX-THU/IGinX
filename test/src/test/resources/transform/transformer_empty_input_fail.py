#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#


class EmptyInputFailTransformer:
    def transform(self, rows):
        if len(rows) == 1:
            raise ValueError("empty input is not supported")
        return rows
