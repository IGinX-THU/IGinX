/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.parquet.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.parquet.exec.RemoteExecutor;
import cn.edu.tsinghua.iginx.parquet.server.ParquetServer;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.StorageProperties;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils.isLocal;

public class ParquetStorage implements IStorage {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStorage.class);

  private Executor executor;

  private ParquetServer server = null;

  private Thread thread = null;

  public ParquetStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.parquet)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }
    if (isLocal(meta)) {
      initLocalStorage(meta);
    } else {
      initRemoteStorage(meta);
    }
  }

  private void initLocalStorage(StorageEngineMeta meta) throws StorageInitializationException {
    Map<String, String> extraParams = meta.getExtraParams();
    String dataDir = extraParams.get("dir");
    String dummyDir = extraParams.get("dummy_dir");
    String dirPrefix = extraParams.get("embedded_prefix");

    StorageProperties storageProperties = StorageProperties.builder().parse(extraParams).build();
    LOGGER.info(
        "storage of {} dir: data_dir={}, dummy_dir={}, dir_prefix={}",
        meta,
        dataDir,
        dummyDir,
        dirPrefix);
    LOGGER.info("storage of {} properties: {}", meta, storageProperties);

    Shared shared = Shared.of(storageProperties);

    this.executor =
        new LocalExecutor(
            shared, meta.isHasData(), meta.isReadOnly(), dataDir, dummyDir, dirPrefix);
    this.server = new ParquetServer(meta.getPort(), executor);
    this.thread = new Thread(server);
    thread.start();
  }

  private void initRemoteStorage(StorageEngineMeta meta) throws StorageInitializationException {
    try {
      this.executor = new RemoteExecutor(meta.getIp(), meta.getPort(), meta.getExtraParams());
    } catch (TTransportException e) {
      throw new StorageInitializationException("encounter error when init RemoteStorage: " + e);
    }
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey())));
    return executor.executeProjectTask(
        project.getPatterns(), project.getTagFilter(), filter, null, dataArea.getStorageUnit(), false);
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
        project.getPatterns(), project.getTagFilter(), filter, null, dataArea.getStorageUnit(), true);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey()),
                select.getFilter()));
    return executor.executeProjectTask(
        project.getPatterns(), project.getTagFilter(), filter, null, dataArea.getStorageUnit(), false);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    KeyInterval keyInterval = dataArea.getKeyInterval();
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, keyInterval.getStartKey()),
                new KeyFilter(Op.L, keyInterval.getEndKey()),
                select.getFilter()));
    return executor.executeProjectTask(
        project.getPatterns(), project.getTagFilter(), filter, null, dataArea.getStorageUnit(), true);
  }

  @Override
  public boolean isSupportProjectWithSetTransform(SetTransform setTransform, DataArea dataArea) {
    // just push down in full column fragment
    KeyInterval keyInterval = dataArea.getKeyInterval();
    if (keyInterval.getStartKey() > 0 || keyInterval.getEndKey() < Long.MAX_VALUE) {
      return false;
    }

    // just push down count(*) for now
    List<FunctionCall> functionCalls = setTransform.getFunctionCallList();
    if (functionCalls.size() != 1) {
      return false;
    }
    FunctionCall functionCall = functionCalls.get(0);
    Function function = functionCall.getFunction();
    FunctionParams params = functionCall.getParams();
    if (function.getFunctionType() != FunctionType.System) {
      return false;
    }
    if (!function.getIdentifier().equals("count")) {
      return false;
    }
    if (params.getPaths().size() != 1) {
      return false;
    }
    String path = params.getPaths().get(0);
    return path.equals("*") || path.equals("*.*");
  }

  @Override
  public TaskExecuteResult executeProjectWithSetTransform(Project project, SetTransform setTransform, DataArea dataArea) {
    if (!isSupportProjectWithSetTransform(setTransform, dataArea)) {
      throw new IllegalArgumentException("unsupported set transform");
    }

    return executor.executeProjectTask(
        project.getPatterns(),
        project.getTagFilter(),
        null,
        setTransform.getFunctionCallList(),
        dataArea.getStorageUnit(),
        false);
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
    return executor.getColumnsOfStorageUnit("*");
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    return executor.getBoundaryOfStorage(prefix);
  }

  @Override
  public synchronized void release() throws PhysicalException {
    executor.close();
    if (thread != null) {
      thread.interrupt();
      thread = null;
    }
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
