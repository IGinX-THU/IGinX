/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.rest;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.exception.StatusCode.STATEMENT_EXECUTION_ERROR;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestSession.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private final ReadWriteLock lock;
  private IginxWorker client;
  private long sessionId;
  private boolean isClosed;
  private int redirectTimes;
  private final String username;
  private final String password;

  public RestSession() {
    this.isClosed = true;
    this.redirectTimes = 0;
    this.lock = new ReentrantReadWriteLock();
    this.username = config.getUsername();
    this.password = config.getPassword();
  }

  private OpenSessionResp tryOpenSession() {
    client = IginxWorker.getInstance();
    OpenSessionReq req = new OpenSessionReq();
    req.username = this.username;
    req.password = this.password;
    return client.openSession(req);
  }

  private void tryCloseSession() {
    CloseSessionReq req = new CloseSessionReq(sessionId);
    client.closeSession(req);
  }

  public synchronized void openSession() throws SessionException {
    if (!isClosed) {
      return;
    }

    do {
      OpenSessionResp resp = tryOpenSession();

      if (StatusUtils.verifyNoRedirect(resp.status)) {
        sessionId = resp.getSessionId();
        break;
      }

      String[] targetAddress = resp.status.getMessage().split(":");
      if (targetAddress.length != 2) {
        throw new SessionException("unexpected redirect address " + resp.status.getMessage());
      }
      LOGGER.info("当前请求将被重定向到：{}", resp.status.getMessage());
      redirectTimes += 1;

    } while (redirectTimes <= Constants.MAX_REDIRECT_TIME);

    if (redirectTimes > Constants.MAX_REDIRECT_TIME) {
      throw new SessionException("重定向次数过多！");
    }
    redirectTimes = 0;

    isClosed = false;
  }

  public synchronized void closeSession() {
    if (isClosed) {
      return;
    }
    try {
      tryCloseSession();
    } finally {
      isClosed = true;
    }
  }

  private synchronized boolean checkRedirect(Status status) {
    return false;
  }

  public void addStorageEngine(
      String ip, int port, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException {
    StorageEngine storageEngine = new StorageEngine(ip, port, type, extraParams);
    AddStorageEnginesReq req =
        new AddStorageEnginesReq(sessionId, Collections.singletonList(storageEngine));

    Status status;
    do {
      lock.readLock().lock();
      try {
        status = client.addStorageEngines(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(status));
    StatusUtils.verifySuccess(status);
  }

  public void deleteColumn(String path, List<Map<String, List<String>>> tagList)
      throws SessionException {
    deleteColumns(Collections.singletonList(path), tagList);
  }

  public void deleteColumns(List<String> paths, List<Map<String, List<String>>> tagList)
      throws SessionException {
    DeleteColumnsReq req = new DeleteColumnsReq(sessionId, paths);
    if (!tagList.isEmpty()) {
      req.setTagsList(tagList);
      req.setFilterType(TagFilterType.Precise);
    }

    Status status;
    do {
      lock.readLock().lock();
      try {
        status = client.deleteColumns(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(status));
    if (status.code == STATEMENT_EXECUTION_ERROR.getStatusCode()) {
      if (status.message.contains(CLEAR_DUMMY_DATA_CAUTION)) {
        LOGGER.warn(status.message);
        return;
      }
    }
    StatusUtils.verifySuccess(status);
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    insertNonAlignedColumnRecords(
        paths,
        timestamps,
        valuesList,
        dataTypeList,
        tagsList,
        TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision timePrecision)
      throws SessionException {
    if (paths.isEmpty()
        || timestamps.length == 0
        || valuesList.length == 0
        || dataTypeList.isEmpty()) {
      LOGGER.error("Invalid insert request!");
      return;
    }
    if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
      LOGGER.error("The sizes of paths, valuesList and dataTypeList should be equal.");
      return;
    }
    if (tagsList != null && paths.size() != tagsList.size()) {
      LOGGER.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
      return;
    }

    Integer[] index = new Integer[timestamps.length];
    for (int i = 0; i < timestamps.length; i++) {
      index[i] = i;
    }
    Arrays.sort(
        index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(timestamps))::get));
    Arrays.sort(timestamps);
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) valuesList[i])[index[j]];
      }
      valuesList[i] = values;
    }

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = (Object[]) valuesList[i];
      if (values.length != timestamps.length) {
        LOGGER.error("The sizes of timestamps and the element of valuesList should be equal.");
        return;
      }
      valueBufferList.add(ByteUtils.getColumnByteBuffer(values, dataTypeList.get(i)));
      Bitmap bitmap = new Bitmap(timestamps.length);
      for (int j = 0; j < timestamps.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertNonAlignedColumnRecordsReq req = new InsertNonAlignedColumnRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(paths);
    req.setKeys(getByteArrayFromLongArray(timestamps));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(dataTypeList);
    req.setTagsList(tagsList);
    req.setTimePrecision(timePrecision);

    Status status;
    do {
      lock.readLock().lock();
      try {
        status = client.insertNonAlignedColumnRecords(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(status));
    StatusUtils.verifySuccess(status);
  }

  public void insertNonAlignedRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision timePrecision)
      throws SessionException {
    if (paths.isEmpty()
        || timestamps.length == 0
        || valuesList.length == 0
        || dataTypeList.isEmpty()) {
      LOGGER.error("Invalid insert request!");
      return;
    }
    if (paths.size() != dataTypeList.size()) {
      LOGGER.error("The sizes of paths and dataTypeList should be equal.");
      return;
    }
    if (timestamps.length != valuesList.length) {
      LOGGER.error("The sizes of timestamps and valuesList should be equal.");
      return;
    }
    if (tagsList != null && paths.size() != tagsList.size()) {
      LOGGER.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
      return;
    }

    Integer[] index = new Integer[timestamps.length];
    for (int i = 0; i < timestamps.length; i++) {
      index[i] = i;
    }
    Arrays.sort(
        index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(timestamps))::get));
    Arrays.sort(timestamps);
    Object[] sortedValuesList = new Object[valuesList.length];
    for (int i = 0; i < valuesList.length; i++) {
      sortedValuesList[i] = valuesList[index[i]];
    }

    index = new Integer[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(paths::get));
    Collections.sort(paths);
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedAttributesList = new ArrayList<>();
    for (int i = 0; i < sortedValuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) sortedValuesList[i])[index[j]];
      }
      sortedValuesList[i] = values;
    }
    for (Integer i : index) {
      sortedDataTypeList.add(dataTypeList.get(i));
    }
    if (tagsList != null) {
      for (Integer i : index) {
        sortedAttributesList.add(tagsList.get(i));
      }
    }

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < timestamps.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      if (values.length != paths.size()) {
        LOGGER.error("The sizes of paths and the element of valuesList should be equal.");
        return;
      }
      valueBufferList.add(ByteUtils.getRowByteBuffer(values, sortedDataTypeList));
      Bitmap bitmap = new Bitmap(values.length);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertNonAlignedRowRecordsReq req = new InsertNonAlignedRowRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(paths);
    req.setKeys(getByteArrayFromLongArray(timestamps));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(sortedDataTypeList);
    req.setTagsList(sortedAttributesList);
    req.setTimePrecision(timePrecision);

    Status status;
    do {
      lock.readLock().lock();
      try {
        status = client.insertNonAlignedRowRecords(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(status));
    StatusUtils.verifySuccess(status);
  }

  public void deleteDataInColumn(
      String path, List<Map<String, List<String>>> tagList, long startKey, long endKey)
      throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(path);
    deleteDataInColumns(paths, tagList, startKey, endKey);
  }

  public void deleteDataInColumns(
      List<String> paths, List<Map<String, List<String>>> tagList, long startKey, long endKey)
      throws SessionException {
    deleteDataInColumns(paths, tagList, startKey, endKey, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
  }

  public void deleteDataInColumns(
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey,
      TimePrecision timePrecision)
      throws SessionException {
    DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, paths, startKey, endKey);
    if (!tagList.isEmpty()) {
      req.setTagsList(tagList);
      req.setFilterType(TagFilterType.Or);
    }
    req.setTimePrecision(timePrecision);

    Status status;
    do {
      lock.readLock().lock();
      try {
        status = client.deleteDataInColumns(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(status));
    if (status.code == STATEMENT_EXECUTION_ERROR.getStatusCode()) {
      if (status.message.contains(CLEAR_DUMMY_DATA_CAUTION)) {
        LOGGER.warn(status.message);
        return;
      }
    }
    StatusUtils.verifySuccess(status);
  }

  public SessionQueryDataSet queryData(
      List<String> paths, long startKey, long endKey, List<Map<String, List<String>>> tagList) {
    return queryData(paths, startKey, endKey, tagList, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
  }

  public SessionQueryDataSet queryData(
      List<String> paths,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagList,
      TimePrecision timePrecision) {
    if (paths.isEmpty() || startKey > endKey) {
      LOGGER.error("Invalid query request!");
      return null;
    }
    QueryDataReq req = new QueryDataReq(sessionId, paths, startKey, endKey);
    if (!tagList.isEmpty()) {
      req.setTagsList(tagList);
      req.setFilterType(TagFilterType.And);
    }
    req.setTimePrecision(timePrecision);

    QueryDataResp resp;

    do {
      lock.readLock().lock();
      try {
        resp = client.queryData(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(resp.status));

    return new SessionQueryDataSet(resp);
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagList,
      AggregateType aggregateType,
      TimePrecision timePrecision) {
    AggregateQueryReq req =
        new AggregateQueryReq(sessionId, paths, startKey, endKey, aggregateType);
    req.setTagsList(tagList);
    req.setFilterType(TagFilterType.And);
    req.setTimePrecision(timePrecision);

    AggregateQueryResp resp;
    do {
      lock.readLock().lock();
      try {
        resp = client.aggregateQuery(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(resp.status));

    return new SessionAggregateQueryDataSet(resp, aggregateType);
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      long precision,
      TimePrecision timePrecision) {
    DownsampleQueryReq req =
        new DownsampleQueryReq(sessionId, paths, startKey, endKey, aggregateType, precision);
    req.setTagsList(tagList);
    req.setFilterType(TagFilterType.And);
    req.setTimePrecision(timePrecision);

    DownsampleQueryResp resp;

    do {
      lock.readLock().lock();
      try {
        resp = client.downsampleQuery(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(resp.status));

    return new SessionQueryDataSet(resp);
  }

  public SessionQueryDataSet showColumns() {
    ShowColumnsReq req = new ShowColumnsReq(sessionId);

    ShowColumnsResp resp;

    do {
      lock.readLock().lock();
      try {
        resp = client.showColumns(req);
      } finally {
        lock.readLock().unlock();
      }
    } while (checkRedirect(resp.status));

    return new SessionQueryDataSet(resp); // 这个初始化方法没有定义LHZ
  }
}
