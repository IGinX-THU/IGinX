"""
A simple example on how to manipulate file with IGinX
"""
import numpy as np
import cv2

from iginx.session import Session
from iginx.thrift.rpc.ttypes import StorageEngineType

# 读取第一行是列名的csv文件，并将数据存入IGinX
def load_csv_with_header(session: Session):
    resp = session.load_csv(
        "LOAD DATA FROM INFILE \"../test/src/test/resources/fileReadAndWrite/csv/test-with-header"
        ".csv\" AS CSV SKIPPING HEADER INTO ta(key, a, b, c, d);")
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
def load_csv_without_header(session: Session):
    resp = session.load_csv(
        "LOAD DATA FROM INFILE \"../test/src/test/resources/fileReadAndWrite/csv/test"
        ".csv\" AS CSV INTO tb(key, a, b, c, d);")
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

# 以添加文件系统数据引擎的方式，读取图片文件数据流并转换格式
def load_image_file(session: Session):
    add_storage_engine(session)
    # select * from image 将会读取最后一级文件路径为image的目录中的所有文件
    dataset = session.execute_statement("select * from image;", fetch_size=2)
    # 获取到的dataset应当为一行n+1列的表，n为文件夹内图片的数量，第一列为key，默认值为0
    columns = dataset.columns()
    print("image list:")
    for column in columns:
        if not column == "key":
            # 查询出来的列名即为文件名，需要进行特殊字符替换
            print(column.replace(".","/").replace("\\","."), end="\n")

    while dataset.has_more():
        row = dataset.next()
        for i in range(len(row)):
            # 第一列是key，跳过
            if i == 0:
                continue
            # 每一列读取二进制数据然后使用cv转换为图像并逐次显示
            binary_data = row[i]
            nparr = np.frombuffer(binary_data, np.uint8)
            img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            cv2.imshow('Image', img_np)
            cv2.waitKey(0)
            cv2.destroyAllWindows()


def add_storage_engine(session: Session, ip: str = "127.0.0.1", port: int = 6668, type: int = StorageEngineType.filesystem, extra_parmas: dict = {
    "iginx_port": "6888",
    "dummy_dir": "test/src/test/resources/fileReadAndWrite/byteStream/image",
    "has_data": "true",
    "is_read_only": "true"
}):
    try:
        # 参数： 存储引擎的ip(str)，端口(int)，类型(int)，额外参数(字典，根据不同的存储引擎类型，参数不同)
        # 例如：添加文件系统引擎，需要指定iginx_port, dummy_dir，has_data，is_read_only三个参数
        # 具体使用样例参考用户手册
        session.add_storage_engine(ip, port, type, extra_params)
        # 如果不重启IGinX，可以注释掉上面的代码不进行重复的添加引擎操作，此时文件夹内的图像数据依然能够实时修改
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

def load_directory(session: Session):
    dir_path = "../test/src/test/resources/fileReadAndWrite/byteStream"
    session.load_directory(dir_path)

    # 查找刚才添加的图片信息
    dataset = session.execute_statement("SHOW COLUMNS;", fetch_size=2)
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
    b'byteStream.tests1'		b'BINARY'		
    b'byteStream.tests2'		b'BINARY'		
    b'byteStream.tests3'		b'BINARY'		
    b'byteStream.tests4'		b'BINARY'		
    """
    dataset.close()



if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()

    load_csv_with_header(session)
    load_csv_without_header(session)
    load_image_file(session)
    load_directory(session)

    session.close()
