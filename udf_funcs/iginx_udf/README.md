# iginx_udf 模块

提供 python udf 装饰器

## 安装

```bash
# 源码安装
cd 当前目录
pip install -e .
```

## 测试

```bash
# 源码安装
cd 当前目录
pip install -e .
pytest -v -s
```

## 使用

用户提供 python 脚本例子：

```python
from iginx_udf import UDTFWrapper, UDAFWrapper, UDSFWrapper

@UDTFWrapper
class MyUDF:
    def eval(self, data:list):
        # do something
        res = process(data)
        return res

# UDAFWrapper, UDSFWrapper 的使用类似
```

## Note

- 使用装饰器而非基类：对用户UDF屏蔽重要的元信息和数据

