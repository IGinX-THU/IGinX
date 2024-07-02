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
 
import io
import os
import sys
import warnings
import xml.etree.ElementTree as ET

from setuptools import setup, find_packages

if sys.version_info >= (3, 12):
    fmt = "IGinX-PyClient may not yet support Python {}.{}."
    warnings.warn(
        fmt.format(*sys.version_info[:2]),
        RuntimeWarning)
    del fmt

tree = ET.parse('../../pom.xml')
root = tree.getroot()
namespaces = {'mvn': 'http://maven.apache.org/POM/4.0.0'}
properties = root.find('mvn:properties', namespaces)
revision = properties.find('mvn:revision', namespaces).text

with open('requirements.txt') as f:
    required = f.read().splitlines()

this_directory = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='iginx-pyclient',
    version=revision,
    include_package_data=True,
    packages=find_packages(),
    author='THU IGinX',
    license="Apache License 2.0",
    author_email='TSIginX@gmail.com',
    python_requires='>=3.8',
    install_requires=required,
    description='IGinX-PyClient',
    long_description=long_description,
    long_description_content_type='text/markdown',
)
