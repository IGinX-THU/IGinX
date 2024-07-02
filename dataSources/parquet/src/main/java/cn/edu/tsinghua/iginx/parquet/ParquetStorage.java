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
package cn.edu.tsinghua.iginx.parquet;

import static cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils.isLocal;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.parquet.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.parquet.exec.RemoteExecutor;
import cn.edu.tsinghua.iginx.parquet.server.ParquetServer;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.StorageProperties;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        project.getPatterns(), project.getTagFilter(), filter, dataArea.getStorageUnit(), false);
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
        project.getPatterns(), project.getTagFilter(), filter, dataArea.getStorageUnit(), true);
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
        project.getPatterns(), project.getTagFilter(), filter, dataArea.getStorageUnit(), false);
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
        project.getPatterns(), project.getTagFilter(), filter, dataArea.getStorageUnit(), true);
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
