package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class PostgreSQLSessionExample {

  private static final String S1 = "sg.d1.s1";
  private static final String S2 = "sg.d1.s2";
  private static final String S3 = "sg.d2.s3";
  private static final String S4 = "sg.d3.s4";
  private static final long COLUMN_START_KEY = 1L;
  private static final long COLUMN_END_KEY = 10000L;
  private static final long NON_ALIGNED_COLUMN_START_KEY = 10001L;
  private static final long NON_ALIGNED_COLUMN_END_KEY = 20000L;
  private static final long ROW_START_KEY = 20001L;
  private static final long ROW_END_KEY = 30000L;
  private static final long NON_ALIGNED_ROW_START_KEY = 30001L;
  private static final long NON_ALIGNED_ROW_END_KEY = 40000L;
  private static final int INTERVAL = 10;
  private static Session session;

  public static void main(String[] args) throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session

    session.openSession();

    // 列式插入对齐数据
    insertColumnRecords();
    // 行式插入对齐数据
    insertRowRecords();
    queryData();
    deleteDataInColumns();

    // 关闭 Session
    session.closeSession();
  }

  private static void insertColumnRecords() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    int size = (int) (COLUMN_END_KEY - COLUMN_START_KEY + 1);
    long[] timestamps = new long[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = i + COLUMN_START_KEY;
    }

    Object[] valuesList = new Object[4];
    for (long i = 0; i < 4; i++) {
      Object[] values = new Object[size];
      for (long j = 0; j < size; j++) {
        if (i < 2) {
          values[(int) j] = i + j;
        } else {
          values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
        }
      }
      valuesList[(int) i] = values;
    }

    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.LONG);
    }
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.BINARY);
    }

    session.insertColumnRecords(
        paths, timestamps, valuesList, dataTypeList, null, TimePrecision.NS);
  }

  private static void insertRowRecords() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    int size = (int) (ROW_END_KEY - ROW_START_KEY + 1);
    long[] timestamps = new long[size];
    Object[] valuesList = new Object[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = ROW_START_KEY + i;
      Object[] values = new Object[4];
      for (long j = 0; j < 4; j++) {
        if (j < 2) {
          values[(int) j] = i + j;
        } else {
          values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
        }
      }
      valuesList[(int) i] = values;
    }

    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.LONG);
    }
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.BINARY);
    }

    session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
  }

  private static void queryData() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    long startKey = NON_ALIGNED_COLUMN_END_KEY - 100L;
    long endKey = ROW_START_KEY + 100L;

    SessionQueryDataSet dataSet = session.queryData(paths, startKey, endKey);
    dataSet.print();
  }

  private static void deleteDataInColumns() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    long startKey = NON_ALIGNED_COLUMN_END_KEY - 50L;
    long endKey = ROW_START_KEY + 50L;

    session.deleteDataInColumns(paths, startKey, endKey);
  }
}
