package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBBeforeDilatationExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBBeforeDilatationExample.class);
  private static final String COLUMN_D1_S1 = "sg.d1.s1";
  private static final String COLUMN_D2_S2 = "sg.d2.s2";
  private static final String COLUMN_D3_S3 = "sg.d3.s3";
  private static final String COLUMN_D4_S4 = "sg.d4.s4";
  private static final long endTimestamp = 100000000L;
  private static final int insertTimes = 10000;
  private static final int recordPerInsert = 10;
  private static final List<String> paths = new ArrayList<>();
  private static Session session;

  static {
    paths.add(COLUMN_D1_S1);
    paths.add(COLUMN_D2_S2);
    paths.add(COLUMN_D3_S3);
    paths.add(COLUMN_D4_S4);
  }

  public static void main(String[] args) throws Exception {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
    // 插入数据
    insertRecords();
    // 关闭 session
    session.closeSession();
  }

  private static void insertRecords() throws SessionException, InterruptedException {
    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      dataTypeList.add(DataType.LONG);
    }

    for (int i = insertTimes; i > 0; i--) {
      long[] timestamps = new long[recordPerInsert];
      Object[] valuesList = new Object[paths.size()];
      for (int j = 0; j < recordPerInsert; j++) {
        timestamps[j] = endTimestamp - (long) i * recordPerInsert + j;
      }
      for (int k = 0; k < paths.size(); k++) {
        Object[] values = new Object[recordPerInsert];
        for (int j = 0; j < recordPerInsert; j++) {
          values[j] = (long) (i + j + k);
        }
        valuesList[k] = values;
      }
      session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
      Thread.sleep(1);
      if ((insertTimes - i + 1) % 100 == 0) {
        LOGGER.info("insert progress: {}/{}.", (insertTimes - i + 1), insertTimes);
      }
    }
  }
}
