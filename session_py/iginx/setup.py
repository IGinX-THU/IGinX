import io
import os
import sys
import warnings

from setuptools import setup, find_packages

if sys.version_info >= (3, 12):
    fmt = "IGinX-PyClient may not yet support Python {}.{}."
    warnings.warn(
        fmt.format(*sys.version_info[:2]),
        RuntimeWarning)
    del fmt

with open('requirements.txt') as f:
    required = f.read().splitlines()

this_directory = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='iginx-pyclient',
    version='0.6.1',
    include_package_data=True,
    packages=find_packages(),
    author='THU IGinX',
    license="GNU GPLv3",
    author_email='TSIginX@gmail.com',
    python_requires='>=3.8',
    install_requires=required,
    description='IGinX-PyClient',
    long_description=long_description,
    long_description_content_type='text/markdown',
)
