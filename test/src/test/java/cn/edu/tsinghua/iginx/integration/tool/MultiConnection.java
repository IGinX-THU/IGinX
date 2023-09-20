package cn.edu.tsinghua.iginx.integration.tool;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.*;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.thrift.TagFilterType;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiConnection {

  private static final Logger logger = LoggerFactory.getLogger(MultiConnection.class);

  private static Session session = null;

  private static SessionPool sessionPool = null;

  public MultiConnection(Session passedSession) {
    session = passedSession;
  }

  public MultiConnection(SessionPool passedSessionPool) {
    sessionPool = passedSessionPool;
  }

  public boolean isSession() {
    return session != null;
  }

  public boolean isSessionPool() {
    return sessionPool != null;
  }

  public boolean isClosed() {
    if (session != null) {
      return session.isClosed();
    }
    if (sessionPool != null) {
      return sessionPool.isClosed();
    }
    return true;
  }

  public void closeSession() throws SessionException {
    if (session != null) {
      session.closeSession();
    }
    if (sessionPool != null) {
      sessionPool.close();
    }
  }

  public void openSession() throws SessionException {
    if (session != null) {
      session.openSession();
    }
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException, ExecutionException {
    if (session != null) {
      session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    } else if (sessionPool != null) {
      sessionPool.insertNonAlignedColumnRecords(
          paths, timestamps, valuesList, dataTypeList, tagsList);
    }
  }

  public void insertColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException, ExecutionException {
    if (session != null) {
      session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    } else if (sessionPool != null) {
      sessionPool.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    }
  }

  public void insertRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException, ExecutionException {
    if (session != null) {
      session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    } else if (sessionPool != null) {
      sessionPool.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    }
  }

  public void insertNonAlignedRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException, ExecutionException {
    if (session != null) {
      session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    } else if (sessionPool != null) {
      sessionPool.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    }
  }

  public void deleteColumns(List<String> paths) throws SessionException, ExecutionException {
    deleteColumns(paths, null, null);
  }

  public void deleteColumns(
      List<String> paths, List<Map<String, List<String>>> tags, TagFilterType type)
      throws SessionException, ExecutionException {
    try {
      if (session != null) {
        session.deleteColumns(paths, tags, type);
      } else if (sessionPool != null) {
        sessionPool.deleteColumns(paths, tags, type);
      }
    } catch (SessionException | ExecutionException e) {
      if (e.toString().trim().equals(CLEAR_DATA_EXCEPTION)) {
        logger.warn(CLEAR_DATA_WARNING);
      } else {
        logger.error(CLEAR_DATA_ERROR, CLEAR_DATA, e.getMessage());
        fail();
      }
    }
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType)
      throws SessionException, ExecutionException {
    if (session != null) {
      return session.aggregateQuery(paths, startKey, endKey, aggregateType);
    }
    if (sessionPool != null) {
      return sessionPool.aggregateQuery(paths, startKey, endKey, aggregateType);
    }
    return null;
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType, long precision)
      throws SessionException, ExecutionException {
    if (session != null) {
      return session.downsampleQuery(paths, startKey, endKey, aggregateType, precision);
    }
    if (sessionPool != null) {
      return sessionPool.downsampleQuery(paths, startKey, endKey, aggregateType, precision);
    }
    return null;
  }

  public SessionQueryDataSet queryData(List<String> paths, long startKey, long endKey)
      throws SessionException, ExecutionException {
    if (session != null) {
      return session.queryData(paths, startKey, endKey);
    }
    if (sessionPool != null) {
      return sessionPool.queryData(paths, startKey, endKey);
    }
    return null;
  }

  public SessionExecuteSqlResult executeSql(String statement)
      throws SessionException, ExecutionException {
    if (session != null) {
      return session.executeSql(statement);
    }
    if (sessionPool != null) {
      return sessionPool.executeSql(statement);
    }
    return null;
  }

  public void addStorageEngine(
      String ip, int port, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException, ExecutionException {
    if (session != null) {
      session.addStorageEngine(ip, port, type, extraParams);
    } else if (sessionPool != null) {
      sessionPool.addStorageEngine(ip, port, type, extraParams);
    }
  }

  public void deleteDataInColumns(List<String> paths, long startKey, long endKey)
      throws SessionException, ExecutionException {
    deleteDataInColumns(paths, startKey, endKey, null, null);
  }

  public void deleteDataInColumns(
      List<String> paths,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tags,
      TagFilterType type)
      throws SessionException, ExecutionException {
    try {
      if (session != null) {
        session.deleteDataInColumns(paths, startKey, endKey, tags, type);
      } else if (sessionPool != null) {
        sessionPool.deleteDataInColumns(paths, startKey, endKey, tags, type);
      }
    } catch (SessionException | ExecutionException e) {
      if (e.toString().trim().equals(CLEAR_DATA_EXCEPTION)) {
        logger.warn(CLEAR_DATA_WARNING);
      } else {
        logger.error(CLEAR_DATA_ERROR, CLEAR_DATA, e.getMessage());
        fail();
      }
    }
  }
}
