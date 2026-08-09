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

from abc import ABC, abstractmethod

"""
Base UDF Wrapper Class
__cls: user's UDF class
_wrapped: instance of user's UDF class
"""
class UDFWrapper(ABC):
    def __init__(self, cls):
        self.__cls = cls

    def __call__(self, *args, **kwargs):
        self._wrapped = self.__cls(*args, **kwargs)
        return self

    def __getattr__(self, item):
        return getattr(self._wrapped, item)

    @abstractmethod
    def transform(self, data, *args, **kwargs):
        """
        called by Java
        """

