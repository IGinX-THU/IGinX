"""
A simple example on how to manipulate file with IGinX
"""
import os

import numpy as np
import cv2

from iginx.session import Session
from iginx.thrift.rpc.ttypes import StorageEngineType


# 读取第一行是列名的csv文件，并将数据存入IGinX
def load_csv_with_header(session: Session, csv_path):
    # 将csv内的数据加载到IGinX数据库，存为ta(key, a, b, c, d)序列，序列的个数与csv列数对应
    resp = session.load_csv(
        f"LOAD DATA FROM INFILE \"{csv_path}\" AS CSV SKIPPING HEADER INTO ta(key, a, b, c, d);")
    columns = resp.columns
    recordsNum = resp.recordsNum
    print(f"Successfully write {recordsNum} record(s) to: {columns}")

    # 查询插入的数据
    dataset = session.execute_statement("select * from ta;", fetch_size=2)
    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()
    """
    key	ta.a	ta.b	ta.c	ta.d	
    0		0		0.5		True		b'aaa'		
    1		1		1.5		False		b'bbb'		
    2		2		2.5		True		b'ccc'		
    3		3		3.5		False		b'ddd'		
    4		4		4.5		True		b'eee'
    """

    dataset.close()


# 读取没有第一行列名的csv文件，并将数据存入IGinX
def load_csv_without_header(session: Session, csv_path):
    # 将csv内的数据加载到IGinX数据库，存为ta(key, a, b, c, d)序列，序列的个数与csv列数对应
    resp = session.load_csv(
        f"LOAD DATA FROM INFILE \"{csv_path}\" AS CSV INTO tb(key, a, b, c, d);")
    columns = resp.columns
    recordsNum = resp.recordsNum
    print(f"Successfully write {recordsNum} record(s) to: {columns}")

    # 查询插入的数据
    dataset = session.execute_statement("select * from tb;", fetch_size=2)
    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()
    """
    key	tb.a	tb.b	tb.c	tb.d	
    0		0		0.5		True		b'aaa'		
    1		1		1.5		False		b'bbb'		
    2		2		2.5		True		b'ccc'		
    3		3		3.5		False		b'ddd'		
    4		4		4.5		True		b'eee'
    """

    dataset.close()


# 以添加文件系统数据引擎的方式，读取图片文件数据流并转换为cv图片
def load_image_file(session: Session, dir_path):
    # 将{dir_path}目录作为文件系统数据库添加到IGinX，目录下有若干图片文件
    add_storage_engine(session, dummy_path=dir_path)

    # 列出该目录下的文件
    # 查找目录下的文件时，每个文件数据对应的列名是[最后一级目录名].[文件名]，注意此时文件名中的.会被替换为\\
    base_dir = os.path.basename(dir_path)
    dataset = session.execute_statement(f"SHOW COLUMNS {base_dir}.*;", fetch_size=2)
    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    file_list = []
    while dataset.has_more():
        row = dataset.next()
        for field in row:
            field_str = str(field)
            if field_str.__contains__("."):
                file_list.append(field_str[field_str.index(".") + 1:-1].replace("\\\\", "\\"))
            print(field_str, end="\t\t")
        print()
    print()
    """
    path	type	
    b'image.image1\\jpg'		b'BINARY'		
    b'image.image2\\jpg'		b'BINARY'	
    结果显示image目录中有image1.jpg和image2.jpg两个文件
    """

    # 读取image目录中的第一个文件，此处是image1.jpg
    dataset = session.execute_statement(f"select {file_list[0]} from {base_dir};", fetch_size=2)

    # 获取到的dataset应当为两列的表，第一列为key，第二列为文件数据
    #       第一行为列名，为“key”和[最后一级目录名].[文件名]，此时文件名中的.会被替换为\\
    columns = dataset.columns()
    print("image list:")
    for column in columns:
        if not column == "key":
            # 查询出来的列名即为文件名，需要进行特殊字符替换
            print(column.replace(".", "/").replace("\\", "."), end="\n")
    """
    image list:
    image/image1.jpg
    """

    # 拼接每一行的文件数据
    binary_data = bytearray()
    while dataset.has_more():
        row = dataset.next()
        for i in range(len(row)):
            # 第一列是key，跳过
            if i == 0:
                continue
            binary_data += row[i]
    # 拼接后的数据转换为图像并显示
    nparr = np.frombuffer(binary_data, np.uint8)
    img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    cv2.imshow('Image', img_np)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    print()


# 将一个文件夹内的所有文件数据加载到IGinX中
def load_directory(session: Session, dir_path):
    # 加载文件夹内所有文件，chunk_size为可选参数，控制每次写入IGinX的数据量，推荐勿改
    session.load_directory(dir_path, chunk_size=10*1024)

    # 查找刚才添加的文件信息，此时文件名中的.被替换为_
    base_dir = os.path.basename(dir_path)
    dataset = session.execute_statement(f"SHOW COLUMNS {base_dir}.*;", fetch_size=2)
    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()
    """
    path	type	
    b'byteStream.test_s1'		b'BINARY'		
    b'byteStream.test_s2'		b'BINARY'		
    b'byteStream.test_s3'		b'BINARY'		
    b'byteStream.test_s4'		b'BINARY'		
    结果显示加载了byteStream目录中的test.s1, test.s2, test.s3, test.s4四个文件
    """
    dataset.close()


# 加载大于1MB的文件，并将数据另存到另一个文件中
def load_largefile_directory_and_export(session: Session, dir_path, out_dir_path):
    # 加载文件数据
    session.load_directory(dir_path, chunk_size=1024*1024)

    # 查找刚才添加的文件信息
    base_dir = os.path.basename(dir_path)
    dataset = session.execute_statement(f"SHOW COLUMNS {base_dir}.*;", fetch_size=2)
    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    file_list = []
    while dataset.has_more():
        row = dataset.next()
        for field in row:
            field_str = str(field)
            if field_str.__contains__("."):
                file_list.append(field_str[field_str.index(".") + 1:-1].replace("\\\\", "\\"))
            print(field_str, end="\t\t")
        print()
    print()
    dataset.close()
    """
    path	type    
    b'largefile.large_img_jpg'		b'BINARY'		
    结果显示添加了largefile目录中的large_img.jpg文件
    """

    # 查看第一个文件在IGinX内存储块数（默认10KB为一块，推荐勿改此参数）
    dataset = session.execute_statement(f"select count({file_list[0]}) from {base_dir};", fetch_size=2)
    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()
    """
    count(largefile.large_img_jpg)	
    284		
    """
    dataset.close()

    file_list[0] = file_list[0][::-1].replace("_", ".", 1)[::-1]
    # 将数据另存到本地文件，注意此处给出的相对路径是相对于本测试脚本的，也可以使用绝对路径
    session.export_to_file(f"select {file_list[0]} from {base_dir} into outfile \"{out_dir_path}\" as stream;")
    # 此时生成的文件路径为./img_outfile/largefile.large_img_jpg


def add_storage_engine(session: Session, ip: str = "127.0.0.1", port: int = 6668,
                       type: int = StorageEngineType.filesystem,
                       extra_params=None, dummy_path = None):
    if extra_params is None:
        extra_params = {
            "iginx_port": "6888",
            "dummy_dir": "test/src/test/resources/fileReadAndWrite/image",
            "has_data": "true",
            "is_read_only": "true"
        }
    if dummy_path:
        extra_params["dummy_dir"] = dummy_path
    try:
        # 参数： 存储引擎的ip(str)，端口(int)，类型(int)，额外参数(字典，根据不同的存储引擎类型，参数不同)
        # 例如：添加文件系统引擎，需要指定iginx_port, dummy_dir，has_data，is_read_only四个参数，此外还有一些可选的可配参数
        # 具体使用样例参考用户手册
        session.add_storage_engine(ip, port, type, extra_params)
    except Exception as e:
        print(e)
    finally:
        # 打印集群信息
        dataset = session.execute_statement("show cluster info;", fetch_size=2)

        columns = dataset.columns()
        for column in columns:
            print(column, end="\t")
        print()

        while dataset.has_more():
            row = dataset.next()
            for field in row:
                print(str(field), end="\t\t")
            print()
        print()

        dataset.close()


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()

    # load_image_file()接收的文件路径参数是相对于IGinX工作目录的
    # load_image_file(session, "test/src/test/resources/fileReadAndWrite/image")
    # 以下函数接收的文件路径参数是相对于本测试脚本的
    # load_csv_with_header(session, "../test/src/test/resources/fileReadAndWrite/csv/test-with-header.csv")
    # load_csv_without_header(session, "../test/src/test/resources/fileReadAndWrite/csv/test.csv")
    # load_directory(session, "../test/src/test/resources/fileReadAndWrite/byteStream")
    load_largefile_directory_and_export(session, "../test/src/test/resources/fileReadAndWrite/image", "img_outfile")

    session.close()
