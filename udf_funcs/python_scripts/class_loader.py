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

from transformer import BaseTransformer


def load_class(file_name, class_name):
    """
    load a sub class of BaseTransformer by file_name and class_name

    return:
    clazz: loaded class
    succ: is class successfully loaded by name
    """
    try:
        import_module = __import__(file_name)
        import_class = getattr(import_module, class_name)

        clazz = import_class()
        return clazz, True
        # if issubclass(import_class, BaseTransformer):
        #     return clazz, True
        # else:
        #     print("The loaded class is not a sub class of BaseTransformer.")
        #     return None, False
    except Exception as e:
        msg = str(e)
        print("Fail to load %s in file %s.py, because %s" % (class_name, file_name, msg))
        return None, False
    pass
