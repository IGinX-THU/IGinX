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

import torch
import numpy as np

from iginx_udf import UDSFWrapper

@UDSFWrapper
class TensorTest():
    """
    测试用的UDF，注意调用时data只能有一列
    """
    def __init__(self):
        pass

    def eval(self, data, args, kvargs):
        some_zeros = np.zeros(40)
        tensor = torch.tensor(some_zeros)
        res = [[f"tensorTest({data[0][1]})"], ["DOUBLE"], [tensor[0].item()]]
        return res
