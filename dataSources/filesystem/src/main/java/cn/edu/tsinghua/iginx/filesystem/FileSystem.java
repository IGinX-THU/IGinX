/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.filesystem;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.filesystem.exec.Executor;
import cn.edu.tsinghua.iginx.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filesystem.exec.RemoteExecutor;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FilePath;
import cn.edu.tsinghua.iginx.filesystem.server.FileSystemServer;
import cn.edu.tsinghua.iginx.filesystem.tools.ConfLoader;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystem implements IStorage {
  private static final String STORAGE_ENGINE = "filesystem";
  public static final int MAXFILESIZE = 100_000_000;
  private static final Logger logger = LoggerFactory.getLogger(FileSystem.class);
  private final StorageEngineMeta meta;
  ExecutorService executorService = Executors.newFixedThreadPool(1); // 为了更好的管理线程
  private Executor executor;
  private String root = ConfLoader.getRootPath();

  public FileSystem(StorageEngineMeta meta)
      throws StorageInitializationException, TTransportException {
    if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }

    boolean isLocal = isLocalIPAddress(meta.getIp());
    if (isLocal) {
      initLocalExecutor(meta);
    } else {
      executor = new RemoteExecutor(meta.getIp(), meta.getPort());
    }
    this.meta = meta;
  }

  public static boolean isLocalIPAddress(String ip) {
    try {
      InetAddress address = InetAddress.getByName(ip);
      if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
        return true;
      }
      NetworkInterface ni = NetworkInterface.getByInetAddress(address);
      if (ni != null && ni.isVirtual()) {
        return true;
      }
      InetAddress local = InetAddress.getLocalHost();
      return local.equals(address);
    } catch (Exception e) {
      return false;
    }
  }

  private void initLocalExecutor(StorageEngineMeta meta) {
    String argRoot = meta.getExtraParams().get("dir");
    root = argRoot == null ? root : FilePath.getRootFromArg(argRoot);

    this.executor = new LocalExecutor(root);

    executorService.submit(new Thread(new FileSystemServer(meta.getPort(), executor)));
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    Filter filter;
    KeyInterval keyInterval = dataArea.getKeyInterval();
    filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executor.executeProjectTask(
        project.getPatterns(),
        project.getTagFilter(),
        FilterTransformer.toFSFilter(filter),
        dataArea.getStorageUnit(),
        false);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executor.executeProjectTask(
        project.getPatterns(),
        project.getTagFilter(),
        FilterTransformer.toFSFilter(filter),
        dataArea.getStorageUnit(),
        true);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executor.executeProjectTask(
        project.getPatterns(),
        project.getTagFilter(),
        FilterTransformer.toFSFilter(select.getFilter()),
        dataArea.getStorageUnit(),
        false);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executor.executeProjectTask(
        project.getPatterns(),
        project.getTagFilter(),
        FilterTransformer.toFSFilter(select.getFilter()),
        dataArea.getStorageUnit(),
        true);
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    return executor.executeDeleteTask(
        delete.getPatterns(),
        delete.getKeyRanges(),
        delete.getTagFilter(),
        dataArea.getStorageUnit());
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    return executor.executeInsertTask(insert.getData(), dataArea.getStorageUnit());
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    return executor.getColumnOfStorageUnit("*");
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    return executor.getBoundaryOfStorage(prefix);
  }

  @Override
  public void release() throws PhysicalException {
    executor.close();
    executorService.shutdown();
  }
}
