package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;

public class OpenTSDBSessionExample {

  private static final String S1 = "sg.d1.s1";
  private static final String S2 = "sg.d1.s2";
  private static final String S3 = "sg.d2.s1";
  private static final String S4 = "sg.d3.s1";
  private static final long COLUMN_START_KEY = 1L;
  private static final long COLUMN_END_KEY = 10L;
  private static final long NON_ALIGNED_COLUMN_START_KEY = 11L;
  private static final long NON_ALIGNED_COLUMN_END_KEY = 20L;
  private static final long ROW_START_KEY = 21L;
  private static final long ROW_END_KEY = 30L;
  private static final long NON_ALIGNED_ROW_START_KEY = 31L;
  private static final long NON_ALIGNED_ROW_END_KEY = 40L;
  private static final int INTERVAL = 10;
  private static Session session;

  public static void main(String[] args)
      throws SessionException, ExecutionException, InterruptedException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    // 打开 Session
    session.openSession();

    insertRowRecords();

    showColumns();

    //        queryData();

    //        deleteDataInColumns();

    session.closeSession();
  }

  private static void insertRowRecords() throws SessionException, ExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    int size = (int) (ROW_END_KEY - ROW_START_KEY + 1);
    long[] timestamps = new long[size];
    Object[] valuesList = new Object[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = ROW_START_KEY + i;
      Object[] values = new Object[2];
      for (long j = 0; j < 2; j++) {
        values[(int) j] = i + j;
      }
      valuesList[(int) i] = values;
    }

    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      dataTypeList.add(DataType.LONG);
    }

    session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
  }

  private static void showColumns() throws SessionException, ExecutionException {
    List<Column> columns = session.showColumns();
    for (Column column : columns) {
      System.out.println(
          String.format("Path: %s, Type: %s", column.getPath(), column.getDataType()));
    }
  }

  private static void queryData() throws SessionException, ExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    long startKey = NON_ALIGNED_COLUMN_END_KEY - 100L;
    long endKey = ROW_START_KEY + 100L;

    SessionQueryDataSet dataSet = session.queryData(paths, startKey, endKey);
    dataSet.print();
  }

  private static void deleteDataInColumns() throws SessionException, ExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    long startKey = NON_ALIGNED_COLUMN_END_KEY - 50L;
    long endKey = ROW_START_KEY + 50L;

    session.deleteDataInColumns(paths, startKey, endKey);
  }
}
