/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.session;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;
import static cn.edu.tsinghua.iginx.utils.HostUtils.isLocalHost;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session {

  private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);

  private static final int MAX_REDIRECT_TIME = 3;

  private static final String USERNAME = "root";

  private static final String PASSWORD = "root";

  private final String username;
  private final String password;
  private final ReadWriteLock lock;
  private String host;
  private int port;
  private IService.Iface client;
  private long sessionId;
  private TTransport transport;
  private boolean isClosed;
  private int redirectTimes;

  private static final TimePrecision timeUnit = TimePrecision.NS;

  public Session(String host, int port) {
    this(host, port, USERNAME, PASSWORD);
  }

  public Session(String host, String port) {
    this(host, port, USERNAME, PASSWORD);
  }

  public Session(String host, String port, String username, String password) {
    this(host, Integer.parseInt(port), username, password);
  }

  public Session(String host, int port, String username, String password) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.isClosed = true;
    this.redirectTimes = 0;
    this.lock = new ReentrantReadWriteLock();
  }

  public long getSessionId() {
    return sessionId;
  }

  public boolean isClosed() {
    return isClosed;
  }

  private synchronized boolean checkRedirect(Status status) throws SessionException, TException {
    if (StatusUtils.verifyNoRedirect(status)) {
      redirectTimes = 0;
      return false;
    }

    redirectTimes += 1;
    if (redirectTimes > MAX_REDIRECT_TIME) {
      throw new SessionException("重定向次数过多！");
    }

    lock.writeLock().lock();

    try {
      tryCloseSession();

      while (redirectTimes <= MAX_REDIRECT_TIME) {

        String[] targetAddress = status.getMessage().split(":");
        if (targetAddress.length != 2) {
          throw new SessionException("unexpected redirect address " + status.getMessage());
        }
        LOGGER.info("当前请求将被重定向到：{}", status.getMessage());
        this.host = targetAddress[0];
        this.port = Integer.parseInt(targetAddress[1]);

        OpenSessionResp resp = tryOpenSession();

        if (StatusUtils.verifyNoRedirect(resp.status)) {
          sessionId = resp.getSessionId();
          break;
        }

        status = resp.status;

        redirectTimes += 1;
      }

      if (redirectTimes > MAX_REDIRECT_TIME) {
        throw new SessionException("重定向次数过多！");
      }
    } finally {
      lock.writeLock().unlock();
    }

    return true;
  }

  private OpenSessionResp tryOpenSession() throws SessionException, TException {
    transport = new TSocket(host, port);
    if (!transport.isOpen()) {
      try {
        transport.open();
      } catch (TTransportException e) {
        throw new SessionException(e);
      }
    }

    client = new IService.Client(new TBinaryProtocol(transport));

    OpenSessionReq req = new OpenSessionReq();
    req.setUsername(username);
    req.setPassword(password);

    return client.openSession(req);
  }

  private void tryCloseSession() throws SessionException {
    CloseSessionReq req = new CloseSessionReq(sessionId);
    try {
      client.closeSession(req);
    } catch (TException e) {
      throw new SessionException(e);
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }

  public synchronized void openSession() throws SessionException {
    if (!isClosed) {
      return;
    }

    try {
      do {
        OpenSessionResp resp = tryOpenSession();

        if (StatusUtils.verifyNoRedirect(resp.status)) {
          sessionId = resp.getSessionId();
          break;
        }

        transport.close();

        String[] targetAddress = resp.status.getMessage().split(":");
        if (targetAddress.length != 2) {
          throw new SessionException("unexpected redirect address " + resp.status.getMessage());
        }
        LOGGER.info("当前请求将被重定向到：{}", resp.status.getMessage());

        this.host = targetAddress[0];
        this.port = Integer.parseInt(targetAddress[1]);
        redirectTimes += 1;

      } while (redirectTimes <= MAX_REDIRECT_TIME);

      if (redirectTimes > MAX_REDIRECT_TIME) {
        throw new SessionException("重定向次数过多！");
      }
      redirectTimes = 0;

    } catch (TException e) {
      transport.close();
      throw new SessionException(e);
    }

    isClosed = false;
  }

  public synchronized void closeSession() throws SessionException {
    if (isClosed) {
      return;
    }
    try {
      tryCloseSession();
    } finally {
      isClosed = true;
    }
  }

  private class Reference<V> {
    public V resp;

    public Reference() {
      resp = null;
    }
  }

  @FunctionalInterface
  public interface SessionExecution {
    Status exec() throws TException;
  }

  private void executeWithCheck(SessionExecution proc) throws SessionException {
    try {
      Status status;
      do {
        lock.writeLock().lock();
        try {
          status = proc.exec();
        } finally {
          lock.writeLock().unlock();
        }
      } while (checkRedirect(status));
      StatusUtils.verifySuccess(status);
    } catch (TException e) {
      throw new SessionException(e);
    }
  }

  public void addStorageEngine(
      String ip, int port, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException {
    StorageEngine storageEngine = new StorageEngine(ip, port, type, extraParams);
    addStorageEngines(Collections.singletonList(storageEngine));
  }

  public void addStorageEngines(List<StorageEngine> storageEngines) throws SessionException {
    AddStorageEnginesReq req = new AddStorageEnginesReq(sessionId, storageEngines);
    executeWithCheck(() -> client.addStorageEngines(req));
  }

  public List<Column> showColumns() throws SessionException {
    ShowColumnsReq req = new ShowColumnsReq(sessionId);
    Reference<ShowColumnsResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.showColumns(req)).status);
    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < ref.resp.paths.size(); i++) {
      columns.add(new Column(ref.resp.paths.get(i), ref.resp.dataTypeList.get(i)));
    }
    return columns;
  }

  public void deleteColumn(String path) throws SessionException {
    List<String> paths = new ArrayList<>();
    paths.add(path);
    deleteColumns(paths);
  }

  public void deleteColumns(List<String> paths) throws SessionException {
    deleteColumns(paths, null, null);
  }

  public void deleteColumns(
      List<String> paths, List<Map<String, List<String>>> tags, TagFilterType type)
      throws SessionException {
    DeleteColumnsReq req = new DeleteColumnsReq(sessionId, mergeAndSortPaths(paths));
    if (tags != null && !tags.isEmpty()) {
      req.setTagsList(tags);
      if (type != null) {
        req.setFilterType(type);
      }
    }
    executeWithCheck(() -> client.deleteColumns(req));
  }

  public void insertColumnRecords(
      List<String> paths, long[] keys, Object[] valuesList, List<DataType> dataTypeList)
      throws SessionException {
    insertColumnRecords(paths, keys, valuesList, dataTypeList, null, timeUnit);
  }

  public void insertColumnRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    insertColumnRecords(paths, keys, valuesList, dataTypeList, tagsList, timeUnit);
  }

  public void insertColumnRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    if (paths.isEmpty() || keys.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
      LOGGER.error("Invalid insert request!");
      return;
    }
    if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
      LOGGER.error("The sizes of paths, valuesList and dataTypeList should be equal.");
      return;
    }
    if (tagsList != null && !tagsList.isEmpty() && paths.size() != tagsList.size()) {
      LOGGER.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
      return;
    }

    long[] sortedKeys = Arrays.copyOf(keys, keys.length);
    Integer[] index = new Integer[sortedKeys.length];
    for (int i = 0; i < sortedKeys.length; i++) {
      index[i] = i;
    }
    Arrays.sort(
        index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(sortedKeys))::get));
    Arrays.sort(sortedKeys);
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) valuesList[i])[index[j]];
      }
      valuesList[i] = values;
    }

    List<String> sortedPaths = new ArrayList<>(paths);
    index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    Object[] sortedValuesList = new Object[valuesList.length];
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
    for (int i = 0; i < valuesList.length; i++) {
      sortedValuesList[i] = valuesList[index[i]];
      sortedDataTypeList.add(dataTypeList.get(index[i]));
    }
    if (tagsList != null && !tagsList.isEmpty()) {
      for (Integer i : index) {
        sortedTagsList.add(tagsList.get(i));
      }
    }

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < sortedValuesList.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      if (values.length != sortedKeys.length) {
        LOGGER.error("The sizes of keys and the element of valuesList should be equal.");
        return;
      }
      valueBufferList.add(ByteUtils.getColumnByteBuffer(values, sortedDataTypeList.get(i)));
      Bitmap bitmap = new Bitmap(sortedKeys.length);
      for (int j = 0; j < sortedKeys.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertColumnRecordsReq req = new InsertColumnRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(sortedPaths);
    req.setKeys(getByteArrayFromLongArray(sortedKeys));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(sortedDataTypeList);
    req.setTagsList(sortedTagsList);
    req.setTimePrecision(precision);

    executeWithCheck(() -> client.insertColumnRecords(req));
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths, long[] keys, Object[] valuesList, List<DataType> dataTypeList)
      throws SessionException {
    insertNonAlignedColumnRecords(paths, keys, valuesList, dataTypeList, null, timeUnit);
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    insertNonAlignedColumnRecords(paths, keys, valuesList, dataTypeList, tagsList, timeUnit);
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    if (paths.isEmpty() || keys.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
      LOGGER.error("Invalid insert request!");
      return;
    }
    if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
      LOGGER.error("The sizes of paths, valuesList and dataTypeList should be equal.");
      return;
    }
    if (tagsList != null && !tagsList.isEmpty() && paths.size() != tagsList.size()) {
      LOGGER.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
      return;
    }

    long[] sortedKeys = Arrays.copyOf(keys, keys.length);
    Integer[] index = new Integer[sortedKeys.length];
    for (int i = 0; i < sortedKeys.length; i++) {
      index[i] = i;
    }
    Arrays.sort(
        index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(sortedKeys))::get));
    Arrays.sort(sortedKeys);
    for (int i = 0; i < valuesList.length; i++) {
      Object[] values = new Object[index.length];
      for (int j = 0; j < index.length; j++) {
        values[j] = ((Object[]) valuesList[i])[index[j]];
      }
      valuesList[i] = values;
    }

    List<String> sortedPaths = new ArrayList<>(paths);
    index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    Object[] sortedValuesList = new Object[valuesList.length];
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
    for (int i = 0; i < valuesList.length; i++) {
      sortedValuesList[i] = valuesList[index[i]];
      sortedDataTypeList.add(dataTypeList.get(index[i]));
    }
    if (tagsList != null && !tagsList.isEmpty()) {
      for (Integer i : index) {
        sortedTagsList.add(tagsList.get(i));
      }
    }

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < sortedValuesList.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      if (values.length != sortedKeys.length) {
        LOGGER.error("The sizes of keys and the element of valuesList should be equal.");
        return;
      }
      valueBufferList.add(ByteUtils.getColumnByteBuffer(values, sortedDataTypeList.get(i)));
      Bitmap bitmap = new Bitmap(sortedKeys.length);
      for (int j = 0; j < sortedKeys.length; j++) {
        if (values[j] != null) {
          bitmap.mark(j);
        }
      }
      bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
    }

    InsertNonAlignedColumnRecordsReq req = new InsertNonAlignedColumnRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(sortedPaths);
    req.setKeys(getByteArrayFromLongArray(sortedKeys));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(sortedDataTypeList);
    req.setTagsList(sortedTagsList);
    req.setTimePrecision(precision);

    executeWithCheck(() -> client.insertNonAlignedColumnRecords(req));
  }

  public void insertRowRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    insertRowRecords(paths, keys, valuesList, dataTypeList, tagsList, timeUnit);
  }

  public void insertRowRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    if (paths.isEmpty() || keys.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
      LOGGER.error("Invalid insert request!");
      return;
    }
    if (paths.size() != dataTypeList.size()) {
      LOGGER.error("The sizes of paths and dataTypeList should be equal.");
      return;
    }
    if (keys.length != valuesList.length) {
      LOGGER.error("The sizes of keys and valuesList should be equal.");
      return;
    }
    if (tagsList != null && !tagsList.isEmpty() && paths.size() != tagsList.size()) {
      LOGGER.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
      return;
    }

    long[] sortedKeys = Arrays.copyOf(keys, keys.length);
    Integer[] index = new Integer[sortedKeys.length];
    for (int i = 0; i < sortedKeys.length; i++) {
      index[i] = i;
    }
    Arrays.sort(
        index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(sortedKeys))::get));
    Arrays.sort(sortedKeys);
    Object[] sortedValuesList = new Object[valuesList.length];
    for (int i = 0; i < valuesList.length; i++) {
      sortedValuesList[i] = valuesList[index[i]];
    }

    List<String> sortedPaths = new ArrayList<>(paths);
    index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
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
    if (tagsList != null && !tagsList.isEmpty()) {
      for (Integer i : index) {
        sortedTagsList.add(tagsList.get(i));
      }
    }

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < sortedKeys.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      if (values.length != sortedPaths.size()) {
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

    InsertRowRecordsReq req = new InsertRowRecordsReq();
    req.setSessionId(sessionId);
    req.setPaths(sortedPaths);
    req.setKeys(getByteArrayFromLongArray(sortedKeys));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(sortedDataTypeList);
    req.setTagsList(sortedTagsList);
    req.setTimePrecision(precision);

    executeWithCheck(() -> client.insertRowRecords(req));
  }

  public void insertNonAlignedRowRecords(
      List<String> paths, long[] keys, Object[] valuesList, List<DataType> dataTypeList)
      throws SessionException {
    insertNonAlignedRowRecords(paths, keys, valuesList, dataTypeList, null, timeUnit);
  }

  public void insertNonAlignedRowRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    insertNonAlignedRowRecords(paths, keys, valuesList, dataTypeList, tagsList, timeUnit);
  }

  public void insertNonAlignedRowRecords(
      List<String> paths,
      long[] keys,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    if (paths.isEmpty() || keys.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
      LOGGER.error("Invalid insert request!");
      return;
    }
    if (paths.size() != dataTypeList.size()) {
      LOGGER.error("The sizes of paths and dataTypeList should be equal.");
      return;
    }
    if (keys.length != valuesList.length) {
      LOGGER.error("The sizes of keys and valuesList should be equal.");
      return;
    }
    if (tagsList != null && !tagsList.isEmpty() && paths.size() != tagsList.size()) {
      LOGGER.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
      return;
    }

    long[] sortedKeys = Arrays.copyOf(keys, keys.length);
    Integer[] index = new Integer[sortedKeys.length];
    for (int i = 0; i < sortedKeys.length; i++) {
      index[i] = i;
    }
    Arrays.sort(
        index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(sortedKeys))::get));
    Arrays.sort(sortedKeys);
    Object[] sortedValuesList = new Object[valuesList.length];
    for (int i = 0; i < valuesList.length; i++) {
      sortedValuesList[i] = valuesList[index[i]];
    }

    List<String> sortedPaths = new ArrayList<>(paths);
    index = new Integer[sortedPaths.size()];
    for (int i = 0; i < sortedPaths.size(); i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparing(sortedPaths::get));
    Collections.sort(sortedPaths);
    List<DataType> sortedDataTypeList = new ArrayList<>();
    List<Map<String, String>> sortedTagsList = new ArrayList<>();
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
    if (tagsList != null && !tagsList.isEmpty()) {
      for (Integer i : index) {
        sortedTagsList.add(tagsList.get(i));
      }
    }

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();
    for (int i = 0; i < sortedKeys.length; i++) {
      Object[] values = (Object[]) sortedValuesList[i];
      if (values.length != sortedPaths.size()) {
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
    req.setPaths(sortedPaths);
    req.setKeys(getByteArrayFromLongArray(sortedKeys));
    req.setValuesList(valueBufferList);
    req.setBitmapList(bitmapBufferList);
    req.setDataTypeList(sortedDataTypeList);
    req.setTagsList(sortedTagsList);
    req.setTimePrecision(precision);

    executeWithCheck(() -> client.insertNonAlignedRowRecords(req));
  }

  public void deleteDataInColumn(String path, long startKey, long endKey) throws SessionException {
    List<String> paths = Collections.singletonList(path);
    deleteDataInColumns(paths, startKey, endKey);
  }

  public void deleteDataInColumns(List<String> paths, long startKey, long endKey)
      throws SessionException {
    deleteDataInColumns(paths, startKey, endKey, null, null);
  }

  public void deleteDataInColumns(
      List<String> paths,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TagFilterType type)
      throws SessionException {
    DeleteDataInColumnsReq req =
        new DeleteDataInColumnsReq(sessionId, mergeAndSortPaths(paths), startKey, endKey);

    if (tagsList != null && !tagsList.isEmpty()) {
      req.setTagsList(tagsList);
      if (type != null) {
        req.setFilterType(type);
      }
    }

    executeWithCheck(() -> client.deleteDataInColumns(req));
  }

  public SessionQueryDataSet queryData(List<String> paths, long startKey, long endKey)
      throws SessionException {
    return queryData(paths, startKey, endKey, null, timeUnit);
  }

  public SessionQueryDataSet queryData(
      List<String> paths, long startKey, long endKey, List<Map<String, List<String>>> tagsList)
      throws SessionException {
    return queryData(paths, startKey, endKey, tagsList, timeUnit);
  }

  public SessionQueryDataSet queryData(
      List<String> paths,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws SessionException {
    if (paths.isEmpty() || startKey > endKey) {
      LOGGER.error("Invalid query request!");
      return null;
    }
    QueryDataReq req = new QueryDataReq(sessionId, mergeAndSortPaths(paths), startKey, endKey);

    if (tagsList != null && !tagsList.isEmpty()) {
      req.setTagsList(tagsList);
    }
    req.setTimePrecision(timePrecision);

    Reference<QueryDataResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.queryData(req)).status);

    return new SessionQueryDataSet(ref.resp);
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType)
      throws SessionException {
    return aggregateQuery(paths, startKey, endKey, aggregateType, null, timeUnit);
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      TimePrecision timePrecision)
      throws SessionException {
    return aggregateQuery(paths, startKey, endKey, aggregateType, null, timePrecision);
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      List<Map<String, List<String>>> tagsList)
      throws SessionException {
    return aggregateQuery(paths, startKey, endKey, aggregateType, tagsList, timeUnit);
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws SessionException {
    AggregateQueryReq req =
        new AggregateQueryReq(sessionId, mergeAndSortPaths(paths), startKey, endKey, aggregateType);

    if (tagsList != null && !tagsList.isEmpty()) {
      req.setTagsList(tagsList);
    }
    req.setTimePrecision(timePrecision);

    Reference<AggregateQueryResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.aggregateQuery(req)).status);

    return new SessionAggregateQueryDataSet(ref.resp, aggregateType);
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      long precision,
      TimePrecision timePrecision)
      throws SessionException {
    return downsampleQuery(paths, startKey, endKey, aggregateType, precision, null, timePrecision);
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType, long precision)
      throws SessionException {
    return downsampleQuery(paths, startKey, endKey, aggregateType, precision, null, timeUnit);
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      long precision,
      List<Map<String, List<String>>> tagsList)
      throws SessionException {
    return downsampleQuery(paths, startKey, endKey, aggregateType, precision, tagsList, timeUnit);
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      long precision,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws SessionException {
    DownsampleQueryReq req =
        new DownsampleQueryReq(
            sessionId, mergeAndSortPaths(paths), startKey, endKey, aggregateType, precision);

    if (tagsList != null && !tagsList.isEmpty()) {
      req.setTagsList(tagsList);
    }
    req.setTimePrecision(timePrecision);

    Reference<DownsampleQueryResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.downsampleQuery(req)).status);

    return new SessionQueryDataSet(ref.resp);
  }

  public int getReplicaNum() throws SessionException {
    GetReplicaNumReq req = new GetReplicaNumReq(sessionId);
    Reference<GetReplicaNumResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.getReplicaNum(req)).status);

    return ref.resp.getReplicaNum();
  }

  public SessionExecuteSqlResult executeSql(String statement) throws SessionException {
    ExecuteSqlReq req = new ExecuteSqlReq(sessionId, statement);
    Reference<ExecuteSqlResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.executeSql(req)).status);

    return new SessionExecuteSqlResult(ref.resp);
  }

  public SessionQueryDataSet queryLast(
      List<String> paths, long startKey, TimePrecision timePrecision) throws SessionException {
    return queryLast(paths, startKey, null, timePrecision);
  }

  public SessionQueryDataSet queryLast(List<String> paths, long startKey) throws SessionException {
    return queryLast(paths, startKey, null, timeUnit);
  }

  public SessionQueryDataSet queryLast(
      List<String> paths, long startKey, List<Map<String, List<String>>> tagsList)
      throws SessionException {
    return queryLast(paths, startKey, tagsList, timeUnit);
  }

  public SessionQueryDataSet queryLast(
      List<String> paths,
      long startKey,
      List<Map<String, List<String>>> tagsList,
      TimePrecision timePrecision)
      throws SessionException {
    if (paths.isEmpty()) {
      LOGGER.error("Invalid query request!");
      return null;
    }

    LastQueryReq req = new LastQueryReq(sessionId, mergeAndSortPaths(paths), startKey);
    if (tagsList != null && !tagsList.isEmpty()) {
      req.setTagsList(tagsList);
    }
    req.setTimePrecision(timePrecision);

    Reference<LastQueryResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.lastQuery(req)).status);

    return new SessionQueryDataSet(ref.resp);
  }

  public void addUser(String username, String password, Set<AuthType> auths)
      throws SessionException {
    AddUserReq req = new AddUserReq(sessionId, username, password, auths);
    executeWithCheck(() -> client.addUser(req));
  }

  public void updateUser(String username, String password, Set<AuthType> auths)
      throws SessionException {
    UpdateUserReq req = new UpdateUserReq(sessionId, username);
    if (password != null) {
      req.setPassword(password);
    }
    if (auths != null) {
      req.setAuths(auths);
    }
    executeWithCheck(() -> client.updateUser(req));
  }

  public void deleteUser(String username) throws SessionException {
    DeleteUserReq req = new DeleteUserReq(sessionId, username);
    executeWithCheck(() -> client.deleteUser(req));
  }

  public ClusterInfo getClusterInfo() throws SessionException {
    GetClusterInfoReq req = new GetClusterInfoReq(sessionId);

    Reference<GetClusterInfoResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.getClusterInfo(req)).status);

    return new ClusterInfo(ref.resp);
  }

  // 适用于查询类请求和删除类请求，因为其 paths 可能带有 *
  private List<String> mergeAndSortPaths(List<String> paths) {
    if (paths.stream().anyMatch(x -> x.equals("*"))) {
      List<String> tempPaths = new ArrayList<>();
      tempPaths.add("*");
      return tempPaths;
    }
    List<String> prefixes =
        paths.stream()
            .filter(x -> x.contains("*"))
            .map(x -> x.substring(0, x.indexOf("*")))
            .collect(Collectors.toList());
    if (prefixes.isEmpty()) {
      Collections.sort(paths);
      return paths;
    }
    List<String> mergedPaths = new ArrayList<>();
    for (String path : paths) {
      if (!path.contains("*")) {
        boolean skip = false;
        for (String prefix : prefixes) {
          if (path.startsWith(prefix)) {
            skip = true;
            break;
          }
        }
        if (skip) {
          continue;
        }
      }
      mergedPaths.add(path);
    }
    mergedPaths.sort(String::compareTo);
    return mergedPaths;
  }

  public QueryDataSet executeQuery(String statement) throws SessionException {
    return executeQuery(statement, Integer.MAX_VALUE);
  }

  public QueryDataSet executeQuery(String statement, int fetchSize) throws SessionException {
    ExecuteStatementReq req = new ExecuteStatementReq(sessionId, statement);
    req.setFetchSize(fetchSize);
    Reference<ExecuteStatementResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.executeStatement(req)).status);

    long queryId = ref.resp.getQueryId();
    List<String> columns = ref.resp.getColumns();
    List<DataType> dataTypes = ref.resp.getDataTypeList();
    QueryDataSetV2 dataSetV2 = ref.resp.getQueryDataSet();
    String warningMessage = ref.resp.getWarningMsg();
    String dir = ref.resp.getExportStreamDir();
    ExportCSV exportCSV = ref.resp.getExportCSV();

    return new QueryDataSet(
        this,
        queryId,
        columns,
        dataTypes,
        fetchSize,
        dataSetV2.valuesList,
        dataSetV2.bitmapList,
        warningMessage,
        dir,
        exportCSV);
  }

  Pair<QueryDataSetV2, Boolean> fetchResult(long queryId, int fetchSize) throws SessionException {
    FetchResultsReq req = new FetchResultsReq(sessionId, queryId);
    req.setFetchSize(fetchSize);
    Reference<FetchResultsResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.fetchResults(req)).status);

    return new Pair<>(ref.resp.getQueryDataSet(), ref.resp.isHasMoreResults());
  }

  public void uploadFileChunk(FileChunk chunk) throws SessionException {
    UploadFileReq req = new UploadFileReq(sessionId, chunk);
    Reference<UploadFileResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.uploadFileChunk(req)).status);
  }

  public Pair<List<String>, Long> executeLoadCSV(String statement, String fileName)
      throws SessionException {
    LoadCSVReq req = new LoadCSVReq(sessionId, statement, fileName);
    Reference<LoadCSVResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.loadCSV(req)).status);

    return new Pair<>(ref.resp.getColumns(), ref.resp.getRecordsNum());
  }

  public LoadUDFResp executeRegisterTask(String statement) throws SessionException {
    return executeRegisterTask(statement, !isLocalHost(host));
  }

  public LoadUDFResp executeRegisterTask(String statement, boolean isRemote)
      throws SessionException {
    ExecuteSqlReq req = new ExecuteSqlReq(sessionId, statement);
    req.setRemoteSession(isRemote);
    Reference<ExecuteSqlResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.executeSql(req)).status);

    ExecuteSqlResp res = ref.resp;
    String parseErrorMsg = res.getParseErrorMsg();
    if (parseErrorMsg != null && !parseErrorMsg.isEmpty()) {
      return new LoadUDFResp(RpcUtils.FAILURE.setMessage(parseErrorMsg));
    }
    String path = res.getUDFModulePath();
    File file = new File(path);
    if (!file.isAbsolute()) {
      statement = statement.replace(path, file.getAbsolutePath());
    } else if (!isRemote) {
      return new LoadUDFResp(RpcUtils.SUCCESS);
    }

    if (!file.exists()) {
      throw new InvalidParameterException(path + " does not exist!");
    }

    ByteBuffer moduleBuffer;
    if (isRemote) {
      try {
        moduleBuffer = CompressionUtils.zipToByteBuffer(file);
      } catch (IOException e) {
        return new LoadUDFResp(
            RpcUtils.FAILURE.setMessage(
                String.format(
                    "Failed to compress module and load into buffer. %s", e.getMessage())));
      }
    } else {
      moduleBuffer = ByteBuffer.allocate(0);
    }

    LoadUDFReq newReq = new LoadUDFReq(sessionId, statement, isRemote);
    newReq.setUdfFile(moduleBuffer);
    Reference<LoadUDFResp> newRef = new Reference<>();
    executeWithCheck(() -> (newRef.resp = client.loadUDF(newReq)).status);

    return newRef.resp;
  }

  void closeQuery(long queryId) throws SessionException {
    CloseStatementReq req = new CloseStatementReq(sessionId, queryId);
    executeWithCheck(() -> client.closeStatement(req));
  }

  public long commitTransformJob(String statement) throws SessionException {
    ExecuteSqlReq req = new ExecuteSqlReq(sessionId, statement);
    Reference<ExecuteSqlResp> ref = new Reference<>();
    executeWithCheck(
        () -> (ref.resp = client.executeSql(req)).status); // to resolve filepath from statement

    String parseErrorMsg = ref.resp.getParseErrorMsg();
    if (parseErrorMsg != null && !parseErrorMsg.isEmpty()) {
      throw new SessionException(parseErrorMsg);
    }

    String filepath = ref.resp.getJobYamlPath();
    return commitTransformJobByYaml(filepath);
  }

  public long commitTransformJobByYaml(String filepath) throws SessionException {
    try {
      YAMLReader yamlReader = new YAMLReader(filepath);
      JobFromYAML jobFromYAML = yamlReader.getJobFromYAML();
      CommitTransformJobReq req = jobFromYAML.toCommitTransformJobReq(sessionId);
      Reference<CommitTransformJobResp> ref = new Reference<>();
      executeWithCheck(() -> (ref.resp = client.commitTransformJob(req)).status);
      return ref.resp.getJobId();
    } catch (FileNotFoundException e) {
      throw new InvalidParameterException(filepath + " does not exist!");
    }
  }

  public long commitTransformJob(
      List<TaskInfo> taskInfoList, ExportType exportType, String fileName) throws SessionException {
    return commitTransformJob(taskInfoList, exportType, fileName, null, true);
  }

  public long commitTransformJob(
      List<TaskInfo> taskInfoList, ExportType exportType, String fileName, String schedule)
      throws SessionException {
    return commitTransformJob(taskInfoList, exportType, fileName, schedule, true);
  }

  public long commitTransformJob(
      List<TaskInfo> taskInfoList,
      ExportType exportType,
      String fileName,
      String schedule,
      boolean stopOnFailure)
      throws SessionException {
    CommitTransformJobReq req = new CommitTransformJobReq(sessionId, taskInfoList, exportType);
    if (fileName != null) {
      req.setFileName(fileName);
    }

    if (schedule != null) {
      req.setSchedule(schedule);
    }

    req.setStopOnFailure(stopOnFailure);

    Reference<CommitTransformJobResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.commitTransformJob(req)).status);
    return ref.resp.getJobId();
  }

  public JobState queryTransformJobStatus(long jobId) throws SessionException {
    QueryTransformJobStatusReq req = new QueryTransformJobStatusReq(sessionId, jobId);
    Reference<QueryTransformJobStatusResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.queryTransformJobStatus(req)).status);
    return ref.resp.getJobState();
  }

  /** { jobState : [jobId, jobId,...]} */
  public Map<JobState, List<Long>> showEligibleJob(JobState jobState) throws SessionException {
    ShowEligibleJobReq req = new ShowEligibleJobReq(sessionId);
    req.setJobState(jobState);
    Reference<ShowEligibleJobResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.showEligibleJob(req)).status);
    return ref.resp.getJobStateMap();
  }

  public void cancelTransformJob(long jobId) throws SessionException {
    CancelTransformJobReq req = new CancelTransformJobReq(sessionId, jobId);
    executeWithCheck(() -> client.cancelTransformJob(req));
  }

  public CurveMatchResult curveMatch(
      List<String> paths, long startKey, long endKey, List<Double> curveQuery, long curveUnit)
      throws SessionException {
    CurveMatchReq req =
        new CurveMatchReq(sessionId, paths, startKey, endKey, curveQuery, curveUnit);
    Reference<CurveMatchResp> ref = new Reference<>();
    executeWithCheck(() -> (ref.resp = client.curveMatch(req)).status);
    return new CurveMatchResult(ref.resp.getMatchedKey(), ref.resp.getMatchedPath());
  }

  public void removeStorageEngine(List<RemovedStorageEngineInfo> removedStorageEngineList)
      throws SessionException {
    RemoveStorageEngineReq req = new RemoveStorageEngineReq(sessionId, removedStorageEngineList);
    executeWithCheck(() -> client.removeStorageEngine(req));
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
}
