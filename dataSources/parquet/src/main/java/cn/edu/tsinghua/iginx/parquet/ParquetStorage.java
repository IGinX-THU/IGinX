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
import cn.edu.tsinghua.iginx.parquet.exec.NewExecutor;
import cn.edu.tsinghua.iginx.parquet.exec.RemoteExecutor;
import cn.edu.tsinghua.iginx.parquet.server.ParquetServer;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetStorage implements IStorage {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(ParquetStorage.class);

  private static final String DRIVER_NAME = "org.duckdb.DuckDBDriver";

  private static final String CONN_URL = "jdbc:duckdb:";

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
    if (!testLocalConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }

    Map<String, String> extraParams = meta.getExtraParams();
    String dataDir = extraParams.get("dir");
    String dummyDir = extraParams.get("dummy_dir");
    String dirPrefix = extraParams.get("embedded_prefix");

    Connection connection;
    try {
      connection = DriverManager.getConnection(CONN_URL);
    } catch (SQLException e) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }

    this.executor =
        new NewExecutor(
            connection, meta.isHasData(), meta.isReadOnly(), dataDir, dummyDir, dirPrefix);
    this.server = new ParquetServer(meta.getPort(), executor);
    this.thread = new Thread(server);
    thread.start();
  }

  private void initRemoteStorage(StorageEngineMeta meta) throws StorageInitializationException {
    try {
      this.executor = new RemoteExecutor(meta.getIp(), meta.getPort());
    } catch (TTransportException e) {
      throw new StorageInitializationException(
          "encounter error when init RemoteStorage " + e.getMessage());
    }
  }

  private boolean testLocalConnection() {
    try {
      Class.forName(DRIVER_NAME);
      Connection conn = DriverManager.getConnection(CONN_URL);
      conn.close();
      return true;
    } catch (ClassNotFoundException | SQLException e) {
      return false;
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
    return executor.getBoundaryOfStorage();
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
