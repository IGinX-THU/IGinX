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

from setuptools import setup, find_packages

package_name = "iginx_udf"

setup(
    name=package_name,                # 包名
    version="0.1.0",
    description="UDF tools for IGinX UDFs",
    author='THU IGinX',
    author_email='TSIginX@gmail.com',
    packages=find_packages(where="src"),  # 搜索 src 下的包
    package_dir={"": "src"},              # 指定源码根目录
    python_requires=">=3.8",
)