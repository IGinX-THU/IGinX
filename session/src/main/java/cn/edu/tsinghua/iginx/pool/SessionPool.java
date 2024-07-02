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
package cn.edu.tsinghua.iginx.pool;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_MAX_VAL;
import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_MIN_VAL;
import static java.lang.Math.max;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.*;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPool.class);
  public static final String SESSION_POOL_IS_CLOSED = "Session pool is closed";
  public static final String CLOSE_THE_SESSION_FAILED = "close the session failed.";

  private static final int RETRY = 3;
  private static final int FINAL_RETRY = RETRY - 1;

  final ReentrantLock rLock = new ReentrantLock(false);
  private ConcurrentHashMap<Pair<String, Integer>, Integer> queueMapIndex =
      new ConcurrentHashMap<>();
  private List<ConcurrentLinkedDeque<Session>> queueList = new ArrayList<>();
  // for session whose resultSet is not released.
  private final ConcurrentMap<Session, Session> occupied = new ConcurrentHashMap<>();

  private long waitToGetSessionTimeoutInMs;

  private volatile int size = 0;
  private int maxSize;
  private final int validSessionSize;

  private static final String USERNAME = "root";

  private static final String PASSWORD = "root";

  private static final int THREAD_NUMBER_MINSIZE = 1;

  private static final int MAXSIZE = 10;
  private static long WAITTOGETSESSIONTIMEOUTINMS = 60_000;
  private static int DEFAULTMAXSIZE = 5;

  private List<IginxInfo> iginxList;
  private List<Integer> sessionNum;

  // whether the queue is closed.
  private boolean closed;

  private final List<Long> sessionIDs = new ArrayList<>();

  public SessionPool(String host, int port) {
    this(host, port, USERNAME, PASSWORD, MAXSIZE);
  }

  public SessionPool(String host, String port) {
    this(host, port, USERNAME, PASSWORD);
  }

  public SessionPool(String host, String portString, String username, String password) {
    this(host, Integer.parseInt(portString), username, password, MAXSIZE);
  }

  public SessionPool(
      String host, String portString, String username, String password, int maxsize) {
    this(host, Integer.parseInt(portString), username, password, maxsize);
  }

  public SessionPool(
      String host,
      String portString,
      String username,
      String password,
      int maxsize,
      long waitToGetSessionTimeoutInMs) {
    this(
        host,
        Integer.parseInt(portString),
        username,
        password,
        maxsize,
        waitToGetSessionTimeoutInMs);
  }

  public SessionPool(String host, int port, String user, String password, int maxSize) {
    this(host, port, user, password, maxSize, WAITTOGETSESSIONTIMEOUTINMS);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      long waitToGetSessionTimeoutInMs) {
    this.maxSize = max(maxSize, THREAD_NUMBER_MINSIZE);
    iginxList = new ArrayList<>();
    sessionNum = new ArrayList<>();
    iginxList.add(
        new IginxInfo.Builder().host(host).port(port).user(user).password(password).build());
    sessionNum.add(this.maxSize);
    validSessionSize = sessionNum.size();
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
  }

  public SessionPool(List<IginxInfo> IginxList) {
    this(
        IginxList,
        new ArrayList<Integer>() {
          {
            for (int i = 0; i < IginxList.size(); i++) add(1);
          }
        },
        DEFAULTMAXSIZE,
        WAITTOGETSESSIONTIMEOUTINMS);
  }

  public SessionPool(List<IginxInfo> IginxList, int maxSize) {
    this(
        IginxList,
        new ArrayList<Integer>() {
          {
            for (int i = 0; i < IginxList.size(); i++) add(1);
          }
        },
        maxSize,
        WAITTOGETSESSIONTIMEOUTINMS);
  }

  public SessionPool(List<IginxInfo> IginxList, List<Integer> sessionNum, int maxSize) {
    this(IginxList, sessionNum, maxSize, WAITTOGETSESSIONTIMEOUTINMS);
  }

  public SessionPool(
      List<IginxInfo> iginxList,
      List<Integer> sessionNum,
      int maxSize,
      long waitToGetSessionTimeoutInMs) {
    validSessionSize = sessionNum.size();
    if (sessionNum.size() < iginxList.size()) {
      LOGGER.warn(
          "IGinX list size {}, distributive session size {}, the remaining IGinX will not get session connection",
          iginxList.size(),
          sessionNum.size());
      for (int i = sessionNum.size(); i < iginxList.size(); i++) {
        sessionNum.add(0);
      }
    }
    this.iginxList = iginxList;
    this.sessionNum = sessionNum;
    this.maxSize = max(maxSize, THREAD_NUMBER_MINSIZE);
    this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
  }

  public List<Long> getSessionIDs() {
    return sessionIDs;
  }

  private Session constructSession(int index) {
    IginxInfo iginxInfo = iginxList.get(index);
    return new Session(
        iginxInfo.getHost(), iginxInfo.getPort(), iginxInfo.getUser(), iginxInfo.getPassword());
  }

  private int getIndexOfIginx(int currentSize) {
    int index = currentSize % validSessionSize, times = currentSize / validSessionSize;
    for (int i = 0; i < validSessionSize; i++) {
      if (sessionNum.get(index) >= times) {
        return index;
      } else {
        index++;
        index = index % validSessionSize;
      }
    }
    return currentSize % validSessionSize;
  }

  private Session constructNewSession(int currentSize) {
    // Construct custom Session
    return constructSession(getIndexOfIginx(currentSize));
  }

  public boolean isClosed() {
    return closed;
  }

  private Session constructNewSession(Session oldSession) {
    // Construct custom Session
    return new Session(
        oldSession.getHost(),
        oldSession.getPort(),
        oldSession.getUsername(),
        oldSession.getPassword());
  }

  private Session getSessionFromQueue(int index) {
    int len = iginxList.size(), times = index % len;
    Session session = null;
    if (queueList.size() == 0) return null;
    if (queueList.size() > times) {
      session = queueList.get(times).poll();
    }
    for (ConcurrentLinkedDeque<Session> queue : queueList) {
      session = queue.poll();
      if (session != null) break;
    }

    return session;
  }

  private Session getSession() throws SessionException {

    Session session = getSessionFromQueue(size);
    if (closed) {
      throw new SessionException(SESSION_POOL_IS_CLOSED);
    }
    if (session != null) {
      return session;
    }

    boolean shouldCreate = false;

    long start = System.currentTimeMillis();
    while (session == null) {
      synchronized (this) {
        if (size < maxSize) {
          // we can create more session
          size++;
          shouldCreate = true;
          // but we do it after skip synchronized block because connection a session is
          // time
          // consuming.
          break;
        }

        // we have to wait for someone returns a session.
        try {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "no more sessions can be created, wait... queue.size={}", currentAvailableSize());
          }
          this.wait(1000);
          long timeOut = Math.min(waitToGetSessionTimeoutInMs, 60_000);
          if (System.currentTimeMillis() - start > timeOut) {
            IginxInfo iginxInfo = iginxList.get(getIndexOfIginx(size));
            LOGGER.warn(
                "the SessionPool has wait for {} seconds to get a new connection: {}:{} with {}, {}",
                (System.currentTimeMillis() - start) / 1000,
                iginxInfo.getHost(),
                iginxInfo.getPassword(),
                iginxInfo.getUser(),
                iginxInfo.getPassword());
            LOGGER.warn(
                "current occupied size {}, queue size {}, considered size {} ",
                occupied.size(),
                currentAvailableSize(),
                size);
            if (System.currentTimeMillis() - start > waitToGetSessionTimeoutInMs) {
              throw new SessionException(
                  String.format(
                      "timeout to get a connection from %s:%s",
                      iginxInfo.getHost(), iginxInfo.getPort()));
            }
          }
        } catch (InterruptedException e) {
          // wake up from this.wait(1000) by this.notify()
        }

        session = getSessionFromQueue(size);

        if (closed) {
          throw new SessionException(SESSION_POOL_IS_CLOSED);
        }
      }
    }

    if (shouldCreate) {
      IginxInfo iginxInfo = iginxList.get(getIndexOfIginx(size));
      // create a new one.
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Create a new redirect Session {}, {}", iginxInfo.getUser(), iginxInfo.getPassword());
      }

      session = constructNewSession(size);

      try {
        session.openSession();
        sessionIDs.add(session.getSessionId());
        // avoid someone has called close() the session pool
        synchronized (this) {
          if (closed) {
            // have to release the connection...
            session.closeSession();
            throw new SessionException(SESSION_POOL_IS_CLOSED);
          }
        }
      } catch (SessionException e) {
        // if exception, we will throw the exception.
        // Meanwhile, we have to set size--
        synchronized (this) {
          size--;
          // we do not need to notifyAll as any waited thread can continue to work after
          // waked up.
          this.notify();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("open session failed, reduce the count and notify others...");
          }
        }
        throw e;
      }
    }

    return session;
  }

  public int currentAvailableSize() {
    int len = 0;
    for (ConcurrentLinkedDeque<Session> sessions : queueList) {
      len += sessions.size();
    }
    return len;
  }

  public int currentOccupiedSize() {
    return occupied.size();
  }

  private void putBack(Session session) {
    rLock.lock();
    try {
      queueList.add(
          new ConcurrentLinkedDeque<Session>() {
            {
              push(session);
            }
          });
      queueMapIndex.putIfAbsent(new Pair<>(session.getHost(), session.getPort()), queueList.size());
    } finally {
      rLock.unlock();
    }

    synchronized (this) {
      // we do not need to notifyAll as any waited thread can continue to work after waked up.
      this.notify();
      // comment the following codes as putBack is too frequently called.
      //      if (LOGGER.isTraceEnabled()) {
      //        LOGGER.trace("put a session back and notify others..., queue.size = {}",
      // queue.size());
      //      }
    }
  }

  private void occupy(Session session) {
    occupied.put(session, session);
  }

  private void tryConstructNewSession(Session oldSession) {
    Session session = constructNewSession(oldSession);
    try {
      session.openSession();
      sessionIDs.add(session.getSessionId());
      // avoid someone has called close() the session pool
      synchronized (this) {
        if (closed) {
          // have to release the connection...
          session.closeSession();
          throw new SessionException(SESSION_POOL_IS_CLOSED);
        }
        putBack(session);
        this.notify();
      }
    } catch (SessionException e) {
      synchronized (this) {
        size--;
        // we do not need to notifyAll as any waited thread can continue to work after waked
        // up.
        this.notify();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("open session failed, reduce the count and notify others...");
        }
      }
    }
  }

  public synchronized void close() throws SessionException {
    for (ConcurrentLinkedDeque<Session> sessionsQueue : queueList) {
      try {
        for (Session session : sessionsQueue) session.closeSession();
      } catch (SessionException e) {
        // do nothing
        LOGGER.warn(CLOSE_THE_SESSION_FAILED, e);
      }
    }
    for (Session session : occupied.keySet()) {
      try {
        session.closeSession();
      } catch (SessionException e) {
        // do nothing
        LOGGER.warn(CLOSE_THE_SESSION_FAILED, e);
      }
    }
    sessionIDs.clear();
    LOGGER.info("closing the session pool, cleaning queues...");
    this.closed = true;
    for (ConcurrentLinkedDeque<Session> sessionsQueue : queueList) {
      sessionsQueue.clear();
    }
    occupied.clear();
  }

  private void closeSession(Session session) {
    if (session != null) {
      try {
        session.closeSession();
      } catch (Exception e2) {
        // do nothing. We just want to guarantee the session is closed.
        LOGGER.warn(CLOSE_THE_SESSION_FAILED, e2);
      }
    }
  }

  public void addStorageEngine(
      String ip, int port, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.addStorageEngine(ip, port, type, extraParams);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addStorageEngine failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void addStorageEngines(List<StorageEngine> storageEngines) throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.addStorageEngines(storageEngines);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addStorageEngines failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void removeHistoryDataSource(List<RemovedStorageEngineInfo> removedStorageEngineList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.removeHistoryDataSource(removedStorageEngineList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("remove history data source failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  private void cleanSessionAndMayThrowConnectionException(
      Session session, int times, SessionException e) throws SessionException {
    closeSession(session);
    tryConstructNewSession(session);
    if (times == FINAL_RETRY) {
      throw new SessionException(
          String.format(
              "retry to execute statement on %s:%s failed %d times: %s",
              session.getHost(), session.getPort(), RETRY, e.getMessage()),
          e);
    }
  }

  public List<Column> showColumns() throws SessionException {
    List<Column> ret = new ArrayList<>();
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        ret = session.showColumns();
        putBack(session);
        return ret;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertTablet failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return ret;
  }

  public void deleteColumn(String path) throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteColumn(path);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteColumn failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteColumns(List<String> paths) throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteColumns(paths);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteColumns failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteColumns(
      List<String> paths, List<Map<String, List<String>>> tags, TagFilterType type)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteColumns(paths, tags, type);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteColumns failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertColumnRecords(
      List<String> paths, long[] timestamps, Object[] valuesList, List<DataType> dataTypeList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertColumnRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertColumnRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertColumnRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList, precision);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertColumnRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths, long[] timestamps, Object[] valuesList, List<DataType> dataTypeList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertNonAlignedColumnRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertNonAlignedColumnRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertNonAlignedColumnRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertNonAlignedColumnRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertNonAlignedColumnRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList, precision);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertNonAlignedColumnRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertRowRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precison)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList, precison);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertRowRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertNonAlignedRowRecords(
      List<String> paths, long[] timestamps, Object[] valuesList, List<DataType> dataTypeList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertNonAlignedRowRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertNonAlignedRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertNonAlignedRowRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void insertNonAlignedRowRecords(
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      TimePrecision precision)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertNonAlignedRowRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList, precision);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("insertNonAlignedRowRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteDataInColumn(String path, long startKey, long endKey) throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteDataInColumn(path, startKey, endKey);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteDataInColumn failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteDataInColumns(List<String> paths, long startKey, long endKey)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteDataInColumns(paths, startKey, endKey);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteDataInColumns failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteDataInColumns(
      List<String> paths,
      long startKey,
      long endKey,
      List<Map<String, List<String>>> tagsList,
      TagFilterType type)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteDataInColumns(paths, startKey, endKey, tagsList, type);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteDataInColumns failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public SessionQueryDataSet queryData(List<String> paths, long startKey, long endKey)
      throws SessionException {
    SessionQueryDataSet sessionQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionQueryDataSet = session.queryData(paths, startKey, endKey);
        putBack(session);
        return sessionQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("queryData failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionQueryDataSet;
  }

  public SessionQueryDataSet queryData(
      List<String> paths, long startKey, long endKey, List<Map<String, List<String>>> tagsList)
      throws SessionException {
    SessionQueryDataSet sessionQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionQueryDataSet = session.queryData(paths, startKey, endKey, tagsList);
        putBack(session);
        return sessionQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("queryData failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionQueryDataSet;
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType)
      throws SessionException {
    SessionAggregateQueryDataSet sessionAggregateQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionAggregateQueryDataSet =
            session.aggregateQuery(paths, startKey, endKey, aggregateType);
        putBack(session);
        return sessionAggregateQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("aggregateQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionAggregateQueryDataSet;
  }

  public SessionAggregateQueryDataSet aggregateQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      Map<String, List<String>> tagsList)
      throws SessionException {
    SessionAggregateQueryDataSet sessionAggregateQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionAggregateQueryDataSet =
            session.aggregateQuery(paths, startKey, endKey, aggregateType);
        putBack(session);
        return sessionAggregateQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("aggregateQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionAggregateQueryDataSet;
  }

  // downsample query without time interval
  public SessionQueryDataSet downsampleQuery(
      List<String> paths, AggregateType aggregateType, long precision) throws SessionException {
    return downsampleQuery(paths, KEY_MIN_VAL, KEY_MAX_VAL, aggregateType, precision);
  }

  // downsample query with time interval
  public SessionQueryDataSet downsampleQuery(
      List<String> paths, long startKey, long endKey, AggregateType aggregateType, long precision)
      throws SessionException {
    SessionQueryDataSet sessionQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionQueryDataSet =
            session.downsampleQuery(paths, startKey, endKey, aggregateType, precision);
        putBack(session);
        return sessionQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("downsampleQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionQueryDataSet;
  }

  public SessionQueryDataSet downsampleQuery(
      List<String> paths,
      long startKey,
      long endKey,
      AggregateType aggregateType,
      long precision,
      List<Map<String, List<String>>> tagsList)
      throws SessionException {
    SessionQueryDataSet sessionQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionQueryDataSet =
            session.downsampleQuery(paths, startKey, endKey, aggregateType, precision, tagsList);
        putBack(session);
        return sessionQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("downsampleQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionQueryDataSet;
  }

  public int getReplicaNum() throws SessionException {
    int ret = 0;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        ret = session.getReplicaNum();
        putBack(session);
        return ret;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("getReplicaNum failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return ret;
  }

  public SessionExecuteSqlResult executeSql(String statement) throws SessionException {
    SessionExecuteSqlResult sessionExecuteSqlResult = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionExecuteSqlResult = session.executeSql(statement);
        putBack(session);
        return sessionExecuteSqlResult;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("executeSql failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionExecuteSqlResult;
  }

  public SessionQueryDataSet queryLast(List<String> paths, long startKey) throws SessionException {
    SessionQueryDataSet sessionQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionQueryDataSet = session.queryLast(paths, startKey);
        putBack(session);
        return sessionQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("queryLast failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionQueryDataSet;
  }

  public SessionQueryDataSet queryLast(
      List<String> paths, long startKey, List<Map<String, List<String>>> tagsList)
      throws SessionException {
    SessionQueryDataSet sessionQueryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        sessionQueryDataSet = session.queryLast(paths, startKey, tagsList);
        putBack(session);
        return sessionQueryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("queryLast failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return sessionQueryDataSet;
  }

  public void addUser(String username, String password, Set<AuthType> auths)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.addUser(username, password, auths);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("addUser failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void updateUser(String username, String password, Set<AuthType> auths)
      throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.updateUser(username, password, auths);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("updateUser failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteUser(String username) throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteUser(username);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("deleteUser failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public ClusterInfo getClusterInfo() throws SessionException {
    ClusterInfo clusterInfo = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        clusterInfo = session.getClusterInfo();
        putBack(session);
        return clusterInfo;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("getClusterInfo failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return clusterInfo;
  }

  public QueryDataSet executeQuery(String statement) throws SessionException {
    QueryDataSet queryDataSet = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        queryDataSet = session.executeQuery(statement);
        putBack(session);
        return queryDataSet;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("executeQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return queryDataSet;
  }

  public long commitTransformJob(
      List<TaskInfo> taskInfoList, ExportType exportType, String fileName) throws SessionException {
    long ret = 0;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        ret = session.commitTransformJob(taskInfoList, exportType, fileName);
        putBack(session);
        return ret;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("commitTransformJob failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return ret;
  }

  public JobState queryTransformJobStatus(long jobId) throws SessionException {
    JobState ret = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        ret = session.queryTransformJobStatus(jobId);
        putBack(session);
        return ret;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("queryTransformJobStatus failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return ret;
  }

  public List<Long> showEligibleJob(JobState jobState) throws SessionException {
    List<Long> ret = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        ret = session.showEligibleJob(jobState);
        putBack(session);
        return ret;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("showEligibleJob failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return ret;
  }

  public void cancelTransformJob(long jobId) throws SessionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.cancelTransformJob(jobId);
        putBack(session);
        return;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("cancelTransformJob failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public CurveMatchResult curveMatch(
      List<String> paths, long startKey, long endKey, List<Double> curveQuery, long curveUnit)
      throws SessionException {
    CurveMatchResult ret = null;
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        ret = session.curveMatch(paths, startKey, endKey, curveQuery, curveUnit);
        putBack(session);
        return ret;
      } catch (SessionException e) {
        // TException means the connection is broken, remove it and get a new one.
        LOGGER.warn("curveMatch failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return ret;
  }

  public static class Builder {
    private String host;
    private int port;
    private int maxSize;
    private String user;
    private String password;
    private int fetchSize;
    private long waitToGetSessionTimeoutInMs = 60_000;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder maxSize(int maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public Builder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
      this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public SessionPool build() {
      return new SessionPool(host, port, user, password, maxSize, fetchSize);
    }
  }
}
