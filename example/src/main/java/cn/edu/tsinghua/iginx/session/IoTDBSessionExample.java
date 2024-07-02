package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

public class IoTDBSessionExample {

  private static final String S1 = "sg.d1.s1";
  private static final String S2 = "sg.d1.s2";
  private static final String S3 = "sg.d2.s1";
  private static final String S4 = "sg.d3.s1";
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
    // 列式插入非对齐数据
    insertNonAlignedColumnRecords();
    // 行式插入对齐数据
    insertRowRecords();
    // 行式插入非对齐数据
    insertNonAlignedRowRecords();
    // 查询序列
    showColumns();
    // 查询数据
    queryData();
    // 聚合查询
    aggregateQuery();
    // Last 查询
    lastQuery();
    // 降采样聚合查询
    downsampleQuery();
    // 曲线匹配
    curveMatch();
    // 删除数据
    deleteDataInColumns();
    // 再次查询数据
    queryData();
    // 查看集群信息
    showClusterInfo();

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

    System.out.println("insertColumnRecords...");
    session.insertColumnRecords(
        paths, timestamps, valuesList, dataTypeList, null, TimePrecision.NS);
  }

  private static void insertNonAlignedColumnRecords() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    int size = (int) (NON_ALIGNED_COLUMN_END_KEY - NON_ALIGNED_COLUMN_START_KEY + 1);
    long[] timestamps = new long[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = i + NON_ALIGNED_COLUMN_START_KEY;
    }

    Object[] valuesList = new Object[4];
    for (long i = 0; i < 4; i++) {
      Object[] values = new Object[size];
      for (long j = 0; j < size; j++) {
        if (j >= size - 50) {
          values[(int) j] = null;
        } else {
          if (i < 2) {
            values[(int) j] = i + j;
          } else {
            values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
          }
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

    System.out.println("insertNonAlignedColumnRecords...");
    session.insertNonAlignedColumnRecords(
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

    System.out.println("insertRowRecords...");
    session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null, TimePrecision.NS);
  }

  private static void insertNonAlignedRowRecords() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    int size = (int) (NON_ALIGNED_ROW_END_KEY - NON_ALIGNED_ROW_START_KEY + 1);
    long[] timestamps = new long[size];
    Object[] valuesList = new Object[size];
    for (long i = 0; i < size; i++) {
      timestamps[(int) i] = NON_ALIGNED_ROW_START_KEY + i;
      Object[] values = new Object[4];
      for (long j = 0; j < 4; j++) {
        if ((i + j) % 2 == 0) {
          values[(int) j] = null;
        } else {
          if (j < 2) {
            values[(int) j] = i + j;
          } else {
            values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
          }
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

    System.out.println("insertNonAlignedRowRecords...");
    session.insertNonAlignedRowRecords(
        paths, timestamps, valuesList, dataTypeList, null, TimePrecision.NS);
  }

  private static void showColumns() throws SessionException {
    List<Column> columnList = session.showColumns();
    columnList.forEach(column -> System.out.println(column.toString()));
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

  private static void aggregateQuery() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    long startKey = COLUMN_END_KEY - 100L;
    long endKey = NON_ALIGNED_ROW_START_KEY + 100L;

    // 聚合查询开始
    System.out.println("Aggregate Query: ");

    // MAX
    SessionAggregateQueryDataSet dataSet =
        session.aggregateQuery(paths, startKey, endKey, AggregateType.MAX);
    dataSet.print();

    // MIN
    dataSet = session.aggregateQuery(paths, startKey, endKey, AggregateType.MIN);
    dataSet.print();

    // FIRST_VALUE
    dataSet = session.aggregateQuery(paths, startKey, endKey, AggregateType.FIRST_VALUE);
    dataSet.print();

    // LAST_VALUE
    dataSet = session.aggregateQuery(paths, startKey, endKey, AggregateType.LAST_VALUE);
    dataSet.print();

    // COUNT
    dataSet = session.aggregateQuery(paths, startKey, endKey, AggregateType.COUNT);
    dataSet.print();

    // SUM
    dataSet = session.aggregateQuery(paths, startKey, endKey, AggregateType.SUM);
    dataSet.print();

    // AVG
    dataSet = session.aggregateQuery(paths, startKey, endKey, AggregateType.AVG);
    dataSet.print();

    // 聚合查询结束
    System.out.println("Aggregate Query Finished.");
  }

  private static void lastQuery() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);
    paths.add(S3);
    paths.add(S4);

    SessionQueryDataSet dataSet = session.queryLast(paths, 0L);
    dataSet.print();
  }

  private static void downsampleQuery() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    long startKey = COLUMN_END_KEY - 100L;
    long endKey = NON_ALIGNED_ROW_START_KEY + 100L;

    // 降采样查询开始
    System.out.println("Downsample Query: ");

    // MAX
    SessionQueryDataSet dataSet =
        session.downsampleQuery(paths, startKey, endKey, AggregateType.MAX, INTERVAL * 100L);
    dataSet.print();

    // MIN
    dataSet = session.downsampleQuery(paths, startKey, endKey, AggregateType.MIN, INTERVAL * 100L);
    dataSet.print();

    // FIRST_VALUE
    dataSet =
        session.downsampleQuery(
            paths, startKey, endKey, AggregateType.FIRST_VALUE, INTERVAL * 100L);
    dataSet.print();

    // LAST_VALUE
    dataSet =
        session.downsampleQuery(paths, startKey, endKey, AggregateType.LAST_VALUE, INTERVAL * 100L);
    dataSet.print();

    // COUNT
    dataSet =
        session.downsampleQuery(paths, startKey, endKey, AggregateType.COUNT, INTERVAL * 100L);
    dataSet.print();

    // SUM
    dataSet = session.downsampleQuery(paths, startKey, endKey, AggregateType.SUM, INTERVAL * 100L);
    dataSet.print();

    // AVG
    dataSet = session.downsampleQuery(paths, startKey, endKey, AggregateType.AVG, INTERVAL * 100L);
    dataSet.print();

    // 降采样查询结束
    System.out.println("Downsample Query Finished.");
  }

  private static void curveMatch() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S2);

    long startKey = COLUMN_END_KEY - 100L;
    long endKey = NON_ALIGNED_ROW_START_KEY + 100L;

    double bias = 6.0;
    int queryNum = 30;
    List<Double> queryList = new ArrayList<>();
    for (int i = 0; i < queryNum; i++) {
      queryList.add(startKey + bias + i);
    }
    long curveUnit = 1L;

    CurveMatchResult result = session.curveMatch(paths, startKey, endKey, queryList, curveUnit);
    System.out.println(result.toString());
  }

  private static void deleteDataInColumns() throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(S1);
    paths.add(S3);
    paths.add(S4);

    long startKey = NON_ALIGNED_COLUMN_END_KEY - 50L;
    long endKey = ROW_START_KEY + 50L;

    session.deleteDataInColumns(paths, startKey, endKey);
  }

  public static void showClusterInfo() throws SessionException {
    ClusterInfo clusterInfo = session.getClusterInfo();
    System.out.println(clusterInfo.getIginxInfos());
    System.out.println(clusterInfo.getStorageEngineInfos());
    System.out.println(clusterInfo.getMetaStorageInfos());
    System.out.println(clusterInfo.getLocalMetaStorageInfo());
  }
}
