package cn.edu.tsinghua.iginx.integration.func.session;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionConcurrencyIT extends BaseSessionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionConcurrencyIT.class);

  // TODO: a very suspicious test; somebody should do something
  // TODO: The following test must be added after bug fix
  //     @Test
  public void multiThreadTestBad() throws SessionException, InterruptedException {
    // query test, multithread insert for storage; multithread query
    int mulStQueryLen = 5;
    List<String> mulStPaths = getPaths(currPath, mulStQueryLen);
    SessionConcurrencyIT.MultiThreadTask[] mulStInsertTasks =
        new SessionConcurrencyIT.MultiThreadTask[mulStQueryLen];
    Thread[] mulStInsertThreads = new Thread[mulStQueryLen];
    for (int i = 0; i < mulStQueryLen; i++) {
      mulStInsertTasks[i] =
          new SessionConcurrencyIT.MultiThreadTask(
              1, getPaths(currPath + i, 1), START_KEY, END_KEY, KEY_PERIOD, 1, null, 6888);
      mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
    }
    for (int i = 0; i < mulStQueryLen; i++) {
      mulStInsertThreads[i].start();
    }
    for (int i = 0; i < mulStQueryLen; i++) {
      mulStInsertThreads[i].join();
    }
    Thread.sleep(1000);

    // query
    int queryTaskNum = 4;
    SessionConcurrencyIT.MultiThreadTask[] mulStQueryTasks =
        new SessionConcurrencyIT.MultiThreadTask[queryTaskNum];
    Thread[] mulStQueryThreads = new Thread[queryTaskNum];
    // each query query one storage

    for (int i = 0; i < queryTaskNum; i++) {
      mulStQueryTasks[i] =
          new SessionConcurrencyIT.MultiThreadTask(
              3, mulStPaths, START_KEY, END_KEY + 1, 0, 0, null, 6888);
      mulStQueryThreads[i] = new Thread(mulStQueryTasks[i]);
    }
    for (int i = 0; i < queryTaskNum; i++) {
      mulStQueryThreads[i].start();
    }
    for (int i = 0; i < queryTaskNum; i++) {
      mulStQueryThreads[i].join();
    }
    Thread.sleep(3000);
    // TODO change the simple query and one of the avg query to multithread
    try {
      for (int i = 0; i < queryTaskNum; i++) {
        SessionQueryDataSet dataSet = (SessionQueryDataSet) mulStQueryTasks[i].getQueryDataSet();
        int len = dataSet.getKeys().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(mulStQueryLen, resPaths.size());
        assertEquals(KEY_PERIOD, len);
        assertEquals(KEY_PERIOD, dataSet.getValues().size());
        for (int j = 0; j < len; j++) {
          long timestamp = dataSet.getKeys()[j];
          assertEquals(j + START_KEY, timestamp);
          List<Object> result = dataSet.getValues().get(j);
          for (int k = 0; k < mulStQueryLen; k++) {
            assertEquals(getPathNum(resPaths.get(k)) + timestamp, result.get(k));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      fail();
    }
    // Test max function
    SessionAggregateQueryDataSet mulStMaxDataSet =
        session.aggregateQuery(mulStPaths, START_KEY, END_KEY + 1, AggregateType.MAX);
    List<String> mulStMaxResPaths = mulStMaxDataSet.getPaths();
    Object[] mulStMaxResult = mulStMaxDataSet.getValues();
    assertEquals(mulStQueryLen, mulStMaxResPaths.size());
    assertEquals(mulStQueryLen, mulStMaxDataSet.getValues().length);
    for (int i = 0; i < mulStQueryLen; i++) {
      assertEquals(getPathNum(mulStMaxResPaths.get(i)) + END_KEY, mulStMaxResult[i]);
    }
    // Test avg function
    SessionAggregateQueryDataSet mulStAvgDataSet =
        session.aggregateQuery(mulStPaths, START_KEY, END_KEY + 1, AggregateType.AVG);
    List<String> mulStAvgResPaths = mulStAvgDataSet.getPaths();
    Object[] mulStAvgResult = mulStAvgDataSet.getValues();
    assertEquals(mulStQueryLen, mulStAvgResPaths.size());
    assertEquals(mulStQueryLen, mulStAvgDataSet.getValues().length);
    for (int i = 0; i < mulStQueryLen; i++) {
      assertEquals(
          getPathNum(mulStAvgResPaths.get(i)) + (START_KEY + END_KEY) / 2.0,
          changeResultToDouble(mulStAvgResult[i]),
          delta);
    }
    currPath += mulStQueryLen;
    // query test, multithread insert for time, multithread query
    int mulTimeQueryLen = 5;
    List<String> mulTimePaths = getPaths(currPath, mulTimeQueryLen);
    SessionConcurrencyIT.MultiThreadTask[] mulTimeInsertTasks =
        new SessionConcurrencyIT.MultiThreadTask[mulTimeQueryLen];
    Thread[] mulTimeInsertThreads = new Thread[mulTimeQueryLen];
    for (int i = 0; i < mulTimeQueryLen; i++) {
      mulTimeInsertTasks[i] =
          new SessionConcurrencyIT.MultiThreadTask(
              1,
              mulTimePaths,
              START_KEY + i,
              END_KEY - (4 - i),
              KEY_PERIOD / mulTimeQueryLen,
              mulTimeQueryLen,
              null,
              6888);
      mulTimeInsertThreads[i] = new Thread(mulTimeInsertTasks[i]);
    }
    for (int i = 0; i < mulTimeQueryLen; i++) {
      mulTimeInsertThreads[i].start();
    }
    for (int i = 0; i < mulTimeQueryLen; i++) {
      mulTimeInsertThreads[i].join();
    }
    Thread.sleep(1000);
    // query
    // TODO change the simple query and one of the avg query to multithread
    SessionQueryDataSet mulTimeQueryDataSet =
        session.queryData(mulTimePaths, START_KEY, END_KEY + 1);
    int mulTimeResLen = mulTimeQueryDataSet.getKeys().length;
    List<String> mulTimeQueryResPaths = mulTimeQueryDataSet.getPaths();
    assertEquals(mulTimeQueryLen, mulTimeQueryResPaths.size());
    assertEquals(KEY_PERIOD, mulTimeResLen);
    assertEquals(KEY_PERIOD, mulTimeQueryDataSet.getValues().size());
    for (int i = 0; i < mulTimeResLen; i++) {
      long timestamp = mulTimeQueryDataSet.getKeys()[i];
      assertEquals(i + START_KEY, timestamp);
      List<Object> result = mulTimeQueryDataSet.getValues().get(i);
      for (int j = 0; j < mulTimeQueryLen; j++) {
        assertEquals(getPathNum(mulTimeQueryResPaths.get(j)) + timestamp, result.get(j));
      }
    }

    // Test max function
    SessionAggregateQueryDataSet mulTimeMaxDataSet =
        session.aggregateQuery(mulTimePaths, START_KEY, END_KEY + 1, AggregateType.MAX);
    List<String> mulTimeMaxResPaths = mulTimeMaxDataSet.getPaths();
    Object[] mulTimeMaxResult = mulTimeMaxDataSet.getValues();
    assertEquals(mulTimeQueryLen, mulTimeMaxResPaths.size());
    assertEquals(mulTimeQueryLen, mulTimeMaxDataSet.getValues().length);
    for (int i = 0; i < mulTimeQueryLen; i++) {
      assertEquals(getPathNum(mulTimeMaxResPaths.get(i)) + END_KEY, mulTimeMaxResult[i]);
    }
    // Test avg function
    SessionAggregateQueryDataSet mulTimeAvgDataSet =
        session.aggregateQuery(mulTimePaths, START_KEY, END_KEY + 1, AggregateType.AVG);
    List<String> mulTimeAvgResPaths = mulTimeAvgDataSet.getPaths();
    Object[] mulTimeAvgResult = mulTimeAvgDataSet.getValues();
    assertEquals(mulTimeQueryLen, mulTimeAvgResPaths.size());
    assertEquals(mulTimeQueryLen, mulTimeAvgDataSet.getValues().length);
    for (int i = 0; i < mulTimeQueryLen; i++) {
      assertEquals(
          getPathNum(mulTimeAvgResPaths.get(i)) + (START_KEY + END_KEY) / 2.0,
          changeResultToDouble(mulTimeAvgResult[i]),
          delta);
    }
    currPath += mulTimeQueryLen;

    // multithread delete test, insert in
    if (isAbleToDelete) {
      // for Storage Part delete
      int mulDelPSLen = 5;
      List<String> mulDelPSPaths = getPaths(currPath, mulDelPSLen);
      insertNumRecords(mulDelPSPaths);
      Thread.sleep(1000);
      SessionQueryDataSet beforePSDataSet =
          session.queryData(mulDelPSPaths, START_KEY, END_KEY + 1);
      List<String> bfPSPath = beforePSDataSet.getPaths();
      assertEquals(mulDelPSLen, bfPSPath.size());
      assertEquals(KEY_PERIOD, beforePSDataSet.getValues().size());
      int delPSThreadNum = mulDelPSLen - 1;
      SessionConcurrencyIT.MultiThreadTask[] delPSTasks =
          new SessionConcurrencyIT.MultiThreadTask[delPSThreadNum];
      Thread[] delPSThreads = new Thread[delPSThreadNum];
      for (int i = 0; i < delPSThreadNum; i++) {
        delPSTasks[i] =
            new SessionConcurrencyIT.MultiThreadTask(
                2, getPaths(currPath + i, 1), delStartKey, delEndKey, delKeyPeriod, 1, null, 6888);
        delPSThreads[i] = new Thread(delPSTasks[i]);
      }
      for (int i = 0; i < delPSThreadNum; i++) {
        delPSThreads[i].start();
      }
      for (int i = 0; i < delPSThreadNum; i++) {
        delPSThreads[i].join();
      }
      Thread.sleep(1000);

      // query
      SessionQueryDataSet delPSDataSet = session.queryData(mulDelPSPaths, START_KEY, END_KEY + 1);
      int delPSQueryLen = delPSDataSet.getKeys().length;
      List<String> delPSResPaths = delPSDataSet.getPaths();
      assertEquals(mulDelPSLen, delPSResPaths.size());
      assertEquals(KEY_PERIOD, delPSQueryLen);
      assertEquals(KEY_PERIOD, delPSDataSet.getValues().size());
      for (int i = 0; i < delPSQueryLen; i++) {
        long timestamp = delPSDataSet.getKeys()[i];
        assertEquals(i + START_KEY, timestamp);
        List<Object> result = delPSDataSet.getValues().get(i);
        for (int j = 0; j < mulDelPSLen; j++) {
          if (delStartKey <= timestamp && timestamp < delEndKey) {
            if (getPathNum(delPSResPaths.get(j)) >= currPath + delPSThreadNum) {
              assertEquals(timestamp + getPathNum(delPSResPaths.get(j)), result.get(j));
            } else {
              assertNull(result.get(j));
            }
          } else {
            assertEquals(getPathNum(delPSResPaths.get(j)) + timestamp, result.get(j));
          }
        }
      }

      // Test avg function
      SessionAggregateQueryDataSet delPSAvgDataSet =
          session.aggregateQuery(mulDelPSPaths, START_KEY, END_KEY + 1, AggregateType.AVG);
      List<String> delPSAvgResPaths = delPSAvgDataSet.getPaths();
      Object[] delPSAvgResult = delPSAvgDataSet.getValues();
      assertEquals(mulDelPSLen, delPSAvgResPaths.size());
      assertEquals(mulDelPSLen, delPSAvgDataSet.getValues().length);
      for (int i = 0; i < mulDelPSLen; i++) {
        double avg =
            ((START_KEY + END_KEY) * KEY_PERIOD / 2.0
                    - (delStartKey + delEndKey - 1) * delKeyPeriod / 2.0)
                / (KEY_PERIOD - delKeyPeriod);
        if (getPathNum(delPSAvgResPaths.get(i)) >= currPath + delPSThreadNum) {
          assertEquals(
              getPathNum(delPSAvgResPaths.get(i)) + (START_KEY + END_KEY) / 2.0,
              changeResultToDouble(delPSAvgResult[i]),
              delta);
        } else {
          assertEquals(
              avg + getPathNum(delPSAvgResPaths.get(i)),
              changeResultToDouble(delPSAvgResult[i]),
              delta);
        }
      }

      currPath += mulDelPSLen;

      // for Time Part delete
      int mulDelPTLen = 5;
      List<String> mulDelPTPaths = getPaths(currPath, mulDelPTLen);
      insertNumRecords(mulDelPTPaths);
      Thread.sleep(1000);
      SessionQueryDataSet beforePTDataSet =
          session.queryData(mulDelPTPaths, START_KEY, END_KEY + 1);
      int beforePTLen = beforePTDataSet.getKeys().length;
      List<String> beforePTPaths = beforePTDataSet.getPaths();
      assertEquals(mulDelPTLen, beforePTPaths.size());
      assertEquals(KEY_PERIOD, beforePTLen);
      assertEquals(KEY_PERIOD, beforePTDataSet.getValues().size());
      int delPTThreadNum = 5;
      int delPTPathNum = 4;
      // the deleted paths of the data
      List<String> delPTPaths = getPaths(currPath, delPTPathNum);
      SessionConcurrencyIT.MultiThreadTask[] delPTTasks =
          new SessionConcurrencyIT.MultiThreadTask[delPTThreadNum];
      Thread[] delPTThreads = new Thread[delPTThreadNum];
      long delPTStartKey = START_KEY + KEY_PERIOD / 5;
      long delPTStep = KEY_PERIOD / 10;
      long delPTTimePeriod = delPTStep * delPTThreadNum;
      long delPTEndTime = delPTStartKey + KEY_PERIOD / 10 * delPTThreadNum - 1;
      for (int i = 0; i < delPTThreadNum; i++) {
        delPTTasks[i] =
            new SessionConcurrencyIT.MultiThreadTask(
                2,
                delPTPaths,
                delPTStartKey + delPTStep * i,
                delPTStartKey + delPTStep * (i + 1),
                delPTStep,
                1,
                null,
                6888);
        delPTThreads[i] = new Thread(delPTTasks[i]);
      }
      for (int i = 0; i < delPTThreadNum; i++) {
        delPTThreads[i].start();
      }
      for (int i = 0; i < delPTThreadNum; i++) {
        delPTThreads[i].join();
      }
      Thread.sleep(1000);

      // query
      SessionQueryDataSet delPTDataSet = session.queryData(mulDelPTPaths, START_KEY, END_KEY + 1);
      int delPTQueryLen = delPTDataSet.getKeys().length;
      List<String> delPTResPaths = delPTDataSet.getPaths();
      assertEquals(mulDelPTLen, delPTResPaths.size());
      assertEquals(KEY_PERIOD, delPTQueryLen);
      assertEquals(KEY_PERIOD, delPTDataSet.getValues().size());
      for (int i = 0; i < delPTQueryLen; i++) {
        long timestamp = delPTDataSet.getKeys()[i];
        assertEquals(i + START_KEY, timestamp);
        List<Object> result = delPTDataSet.getValues().get(i);
        for (int j = 0; j < mulDelPTLen; j++) {
          if (delPTStartKey <= timestamp && timestamp <= delPTEndTime) {
            if (getPathNum(delPTResPaths.get(j)) >= currPath + delPTPathNum) {
              assertEquals(timestamp + getPathNum(delPTResPaths.get(j)), result.get(j));
            } else {
              assertNull(result.get(j));
            }
          } else {
            assertEquals(getPathNum(delPTResPaths.get(j)) + timestamp, result.get(j));
          }
        }
      }
      // Test avg function
      SessionAggregateQueryDataSet delPTAvgDataSet =
          session.aggregateQuery(mulDelPTPaths, START_KEY, END_KEY + 1, AggregateType.AVG);
      List<String> delPTAvgResPaths = delPTAvgDataSet.getPaths();
      Object[] delPTAvgResult = delPTAvgDataSet.getValues();
      assertEquals(mulDelPTLen, delPTAvgResPaths.size());
      assertEquals(mulDelPTLen, delPTAvgDataSet.getValues().length);
      for (int i = 0; i < mulDelPTLen; i++) {
        double avg =
            ((START_KEY + END_KEY) * KEY_PERIOD / 2.0
                    - (delPTStartKey + delPTEndTime) * delPTTimePeriod / 2.0)
                / (KEY_PERIOD - delPTTimePeriod);
        if (getPathNum(delPTAvgResPaths.get(i)) >= currPath + delPTPathNum) {
          assertEquals(
              getPathNum(delPTAvgResPaths.get(i)) + (START_KEY + END_KEY) / 2.0,
              changeResultToDouble(delPTAvgResult[i]),
              delta);
        } else {
          assertEquals(
              avg + getPathNum(delPTAvgResPaths.get(i)),
              changeResultToDouble(delPTAvgResult[i]),
              delta);
        }
      }
      currPath += mulDelPTLen;

      // for Storage All delete
      int mulDelASLen = 5;
      List<String> mulDelASPaths = getPaths(currPath, mulDelASLen);
      insertNumRecords(mulDelASPaths);
      Thread.sleep(1000);
      // threadNum must < 5
      int delASThreadNum = 4;
      SessionConcurrencyIT.MultiThreadTask[] delASTasks =
          new SessionConcurrencyIT.MultiThreadTask[delASThreadNum];
      Thread[] delASThreads = new Thread[delASThreadNum];
      for (int i = 0; i < delASThreadNum; i++) {
        delASTasks[i] =
            new SessionConcurrencyIT.MultiThreadTask(
                2, getPaths(currPath + i, 1), START_KEY, END_KEY + 1, KEY_PERIOD, 1, null, 6888);
        delASThreads[i] = new Thread(delASTasks[i]);
      }
      for (int i = 0; i < delASThreadNum; i++) {
        delASThreads[i].start();
      }
      for (int i = 0; i < delASThreadNum; i++) {
        delASThreads[i].join();
      }
      Thread.sleep(1000);
      // query
      SessionQueryDataSet delASDataSet = session.queryData(mulDelASPaths, START_KEY, END_KEY + 1);
      int delASLen = delASDataSet.getKeys().length;
      List<String> delASResPaths = delASDataSet.getPaths();
      assertEquals(mulDelASLen, delASResPaths.size());
      assertEquals(KEY_PERIOD, delASLen);
      assertEquals(KEY_PERIOD, delASDataSet.getValues().size());

      for (int i = 0; i < delASLen; i++) {
        long timestamp = delASDataSet.getKeys()[i];
        assertEquals(i + START_KEY, timestamp);
        List<Object> result = delASDataSet.getValues().get(i);
        for (int j = 0; j < mulDelASLen; j++) {
          if (getPathNum(delASResPaths.get(j)) >= currPath + delASThreadNum) {
            assertEquals(timestamp + getPathNum(delASResPaths.get(j)), result.get(j));
          } else {
            assertNull(result.get(j));
          }
        }
      }

      // Test avg function
      SessionAggregateQueryDataSet delASAvgDataSet =
          session.aggregateQuery(mulDelASPaths, START_KEY, END_KEY + 1, AggregateType.AVG);
      List<String> delASAvgResPaths = delASAvgDataSet.getPaths();
      Object[] delASAvgResult = delASAvgDataSet.getValues();
      assertEquals(mulDelASLen, delASAvgResPaths.size());
      assertEquals(mulDelASLen, delASAvgDataSet.getValues().length);
      for (int i = 0; i < mulDelASLen; i++) {
        if (getPathNum(delASAvgResPaths.get(i)) >= currPath + delASThreadNum) {
          assertEquals(
              getPathNum(delASAvgResPaths.get(i)) + (START_KEY + END_KEY) / 2.0,
              changeResultToDouble(delASAvgResult[i]),
              delta);
        } else {
          //                    assertEquals("null", new String((byte[])
          // delASAvgResult[i]));
          assertTrue(Double.isNaN((Double) delASAvgResult[i]));
        }
      }
      currPath += mulDelASLen;

      // for time All delete
      int mulDelATLen = 5;
      List<String> mulDelATPaths = getPaths(currPath, mulDelATLen);
      insertNumRecords(mulDelATPaths);
      // Thread.sleep(1000);
      int delATPathLen = 4;
      List<String> delATPath = getPaths(currPath, delATPathLen);
      int delATThreadNum = 5;
      long delATStartKey = START_KEY;
      long delATStep = KEY_PERIOD / delATThreadNum;

      SessionConcurrencyIT.MultiThreadTask[] delATTasks =
          new SessionConcurrencyIT.MultiThreadTask[delATThreadNum];
      Thread[] delATThreads = new Thread[delATThreadNum];

      for (int i = 0; i < delATThreadNum; i++) {
        delATTasks[i] =
            new SessionConcurrencyIT.MultiThreadTask(
                2,
                delATPath,
                delATStartKey + delATStep * i,
                delATStartKey + delATStep * (i + 1),
                delATStep,
                1,
                null,
                6888);
        delATThreads[i] = new Thread(delATTasks[i]);
      }
      for (int i = 0; i < delATThreadNum; i++) {
        delATThreads[i].start();
      }
      for (int i = 0; i < delATThreadNum; i++) {
        delATThreads[i].join();
      }
      Thread.sleep(1000);
      // query
      SessionQueryDataSet delATDataSet = session.queryData(mulDelATPaths, START_KEY, END_KEY + 1);
      int delATLen = delATDataSet.getKeys().length;
      List<String> delATResPaths = delATDataSet.getPaths();
      assertEquals(mulDelATLen, delATResPaths.size());
      assertEquals(KEY_PERIOD, delATLen);
      assertEquals(KEY_PERIOD, delATDataSet.getValues().size());
      for (int i = 0; i < delATLen; i++) {
        long timestamp = delATDataSet.getKeys()[i];
        assertEquals(i + START_KEY, timestamp);
        List<Object> result = delATDataSet.getValues().get(i);
        for (int j = 0; j < mulDelATLen; j++) {
          if (getPathNum(delATResPaths.get(j)) >= currPath + delATPathLen) {
            assertEquals(timestamp + getPathNum(delATResPaths.get(j)), result.get(j));
          } else {
            assertNull(result.get(j));
          }
        }
      }

      // Test avg function
      SessionAggregateQueryDataSet delATAvgDataSet =
          session.aggregateQuery(mulDelATPaths, START_KEY, END_KEY + 1, AggregateType.AVG);
      List<String> delATAvgResPaths = delATAvgDataSet.getPaths();
      Object[] delATAvgResult = delATAvgDataSet.getValues();
      assertEquals(mulDelATLen, delATAvgResPaths.size());
      assertEquals(mulDelATLen, delATAvgDataSet.getValues().length);
      for (int i = 0; i < mulDelATLen; i++) {
        if (getPathNum(delATAvgResPaths.get(i)) >= currPath + delATPathLen) {
          assertEquals(
              getPathNum(delATAvgResPaths.get(i)) + (START_KEY + END_KEY) / 2.0,
              changeResultToDouble(delATAvgResult[i]),
              delta);
        } else {
          //                    assertEquals("null", new String((byte[])
          // delATAvgResult[i]));
          assertTrue(Double.isNaN((Double) delATAvgResult[i]));
        }
      }

      currPath += mulDelATLen;
    }
  }

  protected class MultiThreadTask implements Runnable {

    // 1:insert 2:delete 3:query
    private int type;
    private long startKey;
    private long endKey;
    private long pointNum;
    private int step;
    private List<String> path;
    private Object queryDataSet;
    private AggregateType aggregateType;

    private MultiConnection localSession;

    public MultiThreadTask(
        int type,
        List<String> path,
        long startKey,
        long endKey,
        long pointNum,
        int step,
        AggregateType aggrType,
        int portNum)
        throws SessionException {
      this.type = type;
      this.path = new ArrayList<>(path);
      this.startKey = startKey;
      this.endKey = endKey;
      this.pointNum = pointNum;
      this.step = step;
      this.queryDataSet = null;
      this.aggregateType = aggrType;

      this.localSession =
          new MultiConnection(
              new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
      this.localSession.openSession();
    }

    @Override
    public void run() {
      switch (type) {
          // insert
        case 1:
          long[] timestamps = new long[(int) pointNum];
          for (long i = 0; i < pointNum; i++) {
            timestamps[(int) i] = startKey + step * i;
          }
          int pathSize = path.size();
          Object[] valuesList = new Object[pathSize];
          for (int i = 0; i < pathSize; i++) {
            Object[] values = new Object[(int) pointNum];
            for (int j = 0; j < pointNum; j++) {
              values[j] = timestamps[j] + getPathNum(path.get(i));
            }
            valuesList[i] = values;
          }
          List<DataType> dataTypeList = new ArrayList<>();
          for (int i = 0; i < pathSize; i++) {
            dataTypeList.add(DataType.LONG);
          }
          try {
            localSession.insertNonAlignedColumnRecords(
                path, timestamps, valuesList, dataTypeList, null);
          } catch (SessionException e) {
            LOGGER.error("unexpected error: ", e);
          }
          break;
          // delete
        case 2:
          try {
            localSession.deleteDataInColumns(path, startKey, endKey);
          } catch (SessionException e) {
            LOGGER.error("unexpected error: ", e);
          }
          break;
          // query
        case 3:
          try {
            if (aggregateType == null) {
              queryDataSet = localSession.queryData(path, startKey, endKey);
            } else {
              queryDataSet = localSession.aggregateQuery(path, startKey, endKey, aggregateType);
            }
          } catch (SessionException e) {
            LOGGER.error("unexpected error: ", e);
          }
          break;
        default:
          break;
      }
      try {
        if (localSession.isSession()) this.localSession.closeSession();
      } catch (SessionException e) {
        LOGGER.error("unexpected error: ", e);
      }
    }

    public Object getQueryDataSet() {
      return queryDataSet;
    }
  }
}
