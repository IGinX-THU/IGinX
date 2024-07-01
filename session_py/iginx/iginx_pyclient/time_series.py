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
from .thrift.rpc.ttypes import DataType

class TimeSeries(object):

    def __init__(self, path, type):
        self.__path = path
        self.__type = type


    def get_path(self):
        return self.__path


    def get_type(self):
        return self.__type


    def __str__(self):
        return self.__path + " " + DataType._VALUES_TO_NAMES[self.__type]