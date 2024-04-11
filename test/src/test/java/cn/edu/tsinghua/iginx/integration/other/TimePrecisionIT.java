package cn.edu.tsinghua.iginx.integration.other;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimePrecisionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimePrecisionIT.class);

  private static Session session;

  private static final boolean isForSession = true, isForSessionPool = false;

  private static final TimePrecision[] timePrecision =
      new TimePrecision[] {TimePrecision.S, TimePrecision.MS, TimePrecision.US, TimePrecision.NS};

  private static final String[] path =
      new String[] {"sg.d1.s1", "sg.d1.s2", "sg.d2.s1", "sg.d3.s1"};

  // host info
  private static String defaultTestHost = "127.0.0.1";
  private static int defaultTestPort = 6888;
  private static String defaultTestUser = "root";
  private static String defaultTestPass = "root";

  @BeforeClass
  public static void setUp() throws SessionException {
    if (isForSession) {
      session = new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
    } else if (isForSessionPool) {
      // TODO
    }
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  @Before
  public void insertData() throws SessionException {

    for (int i = 0; i < 4; i++) {
      List<String> paths = new ArrayList<>();
      paths.add(path[i]);

      long[] timestamps = new long[1];
      timestamps[0] = 100;

      Object[] valuesList = new Object[1];
      Object[] values = new Object[1];
      values[0] = 5211314L;
      valuesList[0] = values;

      List<DataType> dataTypeList = new ArrayList<>();
      dataTypeList.add(DataType.LONG);

      session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null, timePrecision[i]);
    }
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  @Test
  public void queryTimeS() throws SessionException {
    List<String> paths = new ArrayList<>(Arrays.asList(path).subList(0, 4));

    long startTime = 0L;
    long endTime = 101L;

    SessionQueryDataSet dataSet =
        session.queryData(paths, startTime, endTime, null, TimePrecision.S);
    dataSet.print();

    long[] timeList = dataSet.getKeys();
    for (int i = 0; i < 4; i++) {
      if (timeList[i] == 100 && i == 0
          || timeList[i] == 100000 && i == 1
          || timeList[i] == 100000000 && i == 2
          || timeList[i] == 100000000000L && i == 3) {
      } else {
        fail();
      }
    }
  }

  @Test
  public void queryTimeMS() throws SessionException {
    List<String> paths = new ArrayList<>(Arrays.asList(path).subList(0, 4));

    long startTime = 0L;
    long endTime = 101L;

    SessionQueryDataSet dataSet =
        session.queryData(paths, startTime, endTime, null, TimePrecision.MS);
    dataSet.print();

    long[] timeList = dataSet.getKeys();
    for (int i = 0; i < 4; i++) {
      if (timeList.length <= i
          || timeList[i] == 100 && i == 0
          || timeList[i] == 100000 && i == 1
          || timeList[i] == 100000000 && i == 2) {
      } else {
        fail();
      }
    }
  }

  @Test
  public void queryTimeUS() throws SessionException {
    List<String> paths = new ArrayList<>(Arrays.asList(path).subList(0, 4));

    long startTime = 0L;
    long endTime = 101L;

    SessionQueryDataSet dataSet =
        session.queryData(paths, startTime, endTime, null, TimePrecision.US);
    dataSet.print();

    long[] timeList = dataSet.getKeys();
    for (int i = 0; i < 4; i++) {
      if (timeList.length <= i || timeList[i] == 100 && i == 0 || timeList[i] == 100000 && i == 1) {
      } else {
        fail();
      }
    }
  }

  @Test
  public void queryTimeNS() throws SessionException {
    List<String> paths = new ArrayList<>(Arrays.asList(path).subList(0, 4));

    long startTime = 0L;
    long endTime = 101L;

    SessionQueryDataSet dataSet =
        session.queryData(paths, startTime, endTime, null, TimePrecision.NS);
    dataSet.print();

    long[] timeList = dataSet.getKeys();
    for (int i = 0; i < 4; i++) {
      if (timeList.length <= i || timeList[i] == 100 && i == 0) {
      } else {
        fail();
      }
    }
  }
}
