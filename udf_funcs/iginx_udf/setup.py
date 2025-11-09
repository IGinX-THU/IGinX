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