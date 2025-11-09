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

from iginx_udf import UDTFWrapper, UDAFWrapper, UDSFWrapper

"""
no args
"""
def test_udtf():
    @UDTFWrapper
    class UDF:
        def eval(self, data):
            res = []
            for row in data:
                res.append([i+1 for i in row])
            return res
    udf = UDF()
    result = udf.transform([[1, 2], [3, 4]])
    assert result == [[2, 3], [4, 5]]


def test_udaf():
    @UDAFWrapper
    class UDAF:
        def eval(self, data):
            res = []
            for col in list(map(list, zip(*data))):
                res.append(sum(col))
            return res

    udaf = UDAF()
    result = udaf.transform([[1, 2], [3, 4]])
    assert result == [4, 6]


def test_udsf():
    @UDSFWrapper
    class UDSF:
        def eval(self, data):
            #   1. 对每个元素都乘 2
            #   2. 然后增加一列，为每行的总和
            doubled = [[v * 2 for v in row] for row in data]
            with_sum = [row + [sum(row)] for row in doubled]
            return with_sum

    udsf = UDSF()
    result = udsf.transform([[1, 2], [3, 4]])

    # doubled = [[2, 4], [6, 8]]
    # with_sum = [[2, 4, 6], [6, 8, 14]]
    expected = [[2, 4, 6], [6, 8, 14]]

    assert result == expected

"""
with args
"""

def test_udtf_with_extra_call_args():
    @UDTFWrapper
    class UDF:
        def eval(self, data, add_one=True):
            """
            - 每行每个元素 +1（默认）
            - 可通过 add_one=False 禁用
            """
            if add_one:
                return [[v + 1 for v in row] for row in data]
            else:
                return data

    udf = UDF()

    # 默认行为
    result1 = udf.transform([[1, 2], [3, 4]])
    assert result1 == [[2, 3], [4, 5]]

    # 关闭加一
    result2 = udf.transform([[1, 2], [3, 4]], add_one=False)
    assert result2 == [[1, 2], [3, 4]]

    # 空输入
    result3 = udf.transform([])
    assert result3 == []


def test_udaf_with_extra_call_args():
    @UDAFWrapper
    class UDAF:
        def eval(self, data, multiplier=1):
            """
            - 对每一列求和
            - 每列结果乘以 multiplier
            """
            # 转置后按列求和
            col_sums = [sum(col) * multiplier for col in zip(*data)]
            return col_sums

    udaf = UDAF()

    # 默认行为
    result1 = udaf.transform([[1, 2], [3, 4]])
    # 列求和 => [1+3, 2+4] => [4,6]
    assert result1 == [4, 6]

    # 使用 multiplier
    result2 = udaf.transform([[1, 2], [3, 4]], multiplier=2)
    # => [8, 12]
    assert result2 == [8, 12]
    result2 = udaf.transform([[1, 2], [3, 4]], 2)
    # => [8, 12]
    assert result2 == [8, 12]

    # 单行数据
    result3 = udaf.transform([[10, 20]])
    assert result3 == [10, 20]

    # 空数据
    result4 = udaf.transform([])
    # 这里空表转置后为 []，约定返回 []
    assert result4 == []


def test_udsf_with_extra_call_args():
    @UDSFWrapper
    class UDSF:
        def eval(self, data, scale=1, offset=0, add_sum=False):
            """
            - 每个元素先 *scale 再 +offset
            - 若 add_sum=True，为每行增加一个求和列
            """
            transformed = [[(v * scale) + offset for v in row] for row in data]

            if add_sum:
                transformed = [row + [sum(row)] for row in transformed]

            return transformed

    udsf = UDSF()

    # 默认参数
    result1 = udsf.transform([[1, 2], [3, 4]])
    # scale=1, offset=0
    assert result1 == [[1, 2], [3, 4]]

    # 位置参数传入 scale=2, offset=1
    result2 = udsf.transform([[1, 2], [3, 4]], 2, 1)
    # => v*2+1 => [[3,5],[7,9]]
    assert result2 == [[3, 5], [7, 9]]

    # 使用关键字参数 add_sum=True
    result3 = udsf.transform([[1, 2], [3, 4]], 2, 0, add_sum=True)
    # => [[2,4,6],[6,8,14]]
    assert result3 == [[2, 4, 6], [6, 8, 14]]

    # 仅修改 offset
    result4 = udsf.transform([[1, 2]], offset=10)
    # => [[11,12]]
    assert result4 == [[11, 12]]

    # 空数据情况
    result5 = udsf.transform([], 5)
    assert result5 == []