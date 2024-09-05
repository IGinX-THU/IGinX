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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore;

import static cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils.isLocal;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.common.Configs;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.service.FileStoreConfig;
import cn.edu.tsinghua.iginx.filestore.service.FileStoreService;
import cn.edu.tsinghua.iginx.filestore.service.storage.StorageConfig;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructureManager;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.LegacyParquet;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoreStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreStorage.class);

  private final FileStoreService service;

  private final FileStoreConfig fileStoreConfig;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  private final boolean isLegacyParquet;

  static {
    Collection<FileStructure> structures = FileStructureManager.getInstance().getAll();
    LOGGER.info("found file structures: {}", structures);
  }

  public FileStoreStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.filestore)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }

    String dataStructPath =
        String.join(".", FileStoreConfig.Fields.data, StorageConfig.Fields.struct);
    isLegacyParquet =
        LegacyParquet.NAME.equals(
            meta.getExtraParams().getOrDefault(dataStructPath, LegacyParquet.NAME));

    InetSocketAddress address = new InetSocketAddress(meta.getIp(), meta.getPort());
    this.fileStoreConfig = toFileStoreConfig(meta);
    try {
      this.service = new FileStoreService(address, fileStoreConfig);
    } catch (FileStoreException e) {
      throw new StorageInitializationException("file store initialization error", e);
    }
  }

  static FileStoreConfig toFileStoreConfig(StorageEngineMeta meta)
      throws StorageInitializationException {
    Config rawConfig = toConfig(meta);
    LOGGER.debug("storage of {} config: {}", meta, rawConfig);
    FileStoreConfig fileStoreConfig = FileStoreConfig.of(rawConfig);
    LOGGER.debug("storage of {} will be initialized with {}", meta, fileStoreConfig);
    List<AbstractConfig.ValidationProblem> problems = fileStoreConfig.validate();
    if (!problems.isEmpty()) {
      throw new StorageInitializationException("invalided config: " + problems);
    }
    return fileStoreConfig;
  }

  static Config toConfig(StorageEngineMeta meta) throws StorageInitializationException {
    HashMap<String, String> reshaped = new HashMap<>();

    for (Map.Entry<String, String> param : meta.getExtraParams().entrySet()) {
      String key = param.getKey();
      String value = param.getValue();
      if (key.contains(".")) {
        reshaped.put(key, value);
      }
    }

    Configs.put(reshaped, String.valueOf(isLocal(meta)), FileStoreConfig.Fields.serve);
    Configs.put(
        reshaped,
        meta.getExtraParams().get("dir"),
        FileStoreConfig.Fields.data,
        StorageConfig.Fields.root);
    Configs.put(
        reshaped,
        meta.getExtraParams().get("dummy_dir"),
        FileStoreConfig.Fields.dummy,
        StorageConfig.Fields.root);
    Configs.put(
        reshaped,
        meta.getExtraParams().get("embedded_prefix"),
        FileStoreConfig.Fields.dummy,
        StorageConfig.Fields.config,
        FileTreeConfig.Fields.prefix);
    Configs.putIfAbsent(
        reshaped,
        FileStoreConfig.DEFAULT_DATA_STRUCT,
        FileStoreConfig.Fields.data,
        StorageConfig.Fields.struct);
    Configs.putIfAbsent(
        reshaped,
        FileStoreConfig.DEFAULT_DUMMY_STRUCT,
        FileStoreConfig.Fields.dummy,
        StorageConfig.Fields.struct);

    Config config = ConfigFactory.parseMap(reshaped, "storage engine initialization parameters");

    if (!meta.isHasData()) {
      LOGGER.debug("storage of {} don't have data, ignore config for dummy", meta);
      config = config.withoutPath(FileStoreConfig.Fields.dummy);
    }
    if (meta.isReadOnly()) {
      LOGGER.debug("storage of {} is read only, ignore config for iginx data", meta);
      config = config.withoutPath(FileStoreConfig.Fields.data);
    }

    return config;
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
    return executeQuery(unitOf(dataArea), getDataTargetOf(project, dataArea), null);
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
    return executeQuery(unitOfDummy(), getDataTargetOf(project, dataArea), null);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeQuery(unitOf(dataArea), getDataTargetOf(select, project, dataArea), null);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea dataArea) {
    return executeQuery(unitOfDummy(), getDataTargetOf(select, project, dataArea), null);
  }

  @Override
  public boolean isSupportProjectWithSetTransform(SetTransform setTransform, DataArea dataArea) {
    if (!isLegacyParquet) {
      return false;
    }

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
  public TaskExecuteResult executeProjectWithSetTransform(
      Project project, SetTransform setTransform, DataArea dataArea) {
    if (!isSupportProjectWithSetTransform(setTransform, dataArea)) {
      throw new IllegalArgumentException("unsupported set transform");
    }

    DataArea reshapedDataArea =
        new DataArea(dataArea.getStorageUnit(), KeyInterval.getDefaultKeyInterval());

    return executeQuery(
        unitOf(dataArea), getDataTargetOf(project, reshapedDataArea), AggregateType.COUNT);
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    Filter filter;
    if (delete.getKeyRanges() == null || delete.getKeyRanges().isEmpty()) {
      filter = null;
    } else {
      filter = Filters.toFilter(delete.getKeyRanges());
    }
    try {
      service.delete(
          unitOf(dataArea), new DataTarget(filter, delete.getPatterns(), delete.getTagFilter()));
      return new TaskExecuteResult();
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    try {
      service.insert(unitOf(dataArea), insert.getData());
      return new TaskExecuteResult();
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public List<Column> getColumns(Set<String> patterns, TagFilter tagFilter)
      throws PhysicalException {
    List<Column> columns = Collections.synchronizedList(new ArrayList<>());
    List<Column> columnsWrapper = Collections.synchronizedList(columns);

    Map<DataUnit, DataBoundary> units = service.getUnits(null);

    List<String> patternList = new ArrayList<>(patterns);
    if (patternList.isEmpty()) {
      patternList = null;
    }

    DataTarget dataTarget = new DataTarget(new BoolFilter(false), patternList, tagFilter);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (DataUnit unit : units.keySet()) {
      CompletableFuture<Void> future =
          CompletableFuture.supplyAsync(
                  () -> {
                    List<Column> localColumns = new ArrayList<>();
                    try (RowStream stream = service.query(unit, dataTarget, null)) {
                      Header header = stream.getHeader();
                      for (Field field : header.getFields()) {
                        localColumns.add(
                            new Column(
                                field.getName(), field.getType(), field.getTags(), unit.isDummy()));
                      }
                    } catch (PhysicalException e) {
                      throw new CompletionException(e);
                    }
                    return localColumns;
                  },
                  executor)
              .thenAccept(columnsWrapper::addAll);
      futures.add(future);
    }

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (CompletionException e) {
      throw new FileStoreException(e);
    }

    return columns;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    Map<DataUnit, DataBoundary> units = service.getUnits(Strings.emptyToNull(prefix));
    DataBoundary boundary = units.get(unitOfDummy());
    if (Objects.equals(boundary, new DataBoundary())) {
      throw new PhysicalTaskExecuteFailureException("no data");
    }
    ColumnsInterval columnsInterval =
        new ColumnsInterval(boundary.getStartColumn(), boundary.getEndColumn());
    KeyInterval keyInterval = new KeyInterval(boundary.getStartKey(), boundary.getEndKey());
    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void release() throws PhysicalException {
    service.close();
    executor.shutdown();
  }

  private static DataUnit unitOf(DataArea dataArea) {
    DataUnit dataUnit = new DataUnit();
    dataUnit.setDummy(false);
    dataUnit.setName(dataArea.getStorageUnit());
    return dataUnit;
  }

  private static DataUnit unitOfDummy() {
    DataUnit dataUnit = new DataUnit();
    dataUnit.setDummy(true);
    return dataUnit;
  }

  private static DataTarget getDataTargetOf(Project project, DataArea dataArea) {
    Filter filter = Filters.toFilter(dataArea.getKeyInterval());
    return new DataTarget(filter, project.getPatterns(), project.getTagFilter());
  }

  private static DataTarget getDataTargetOf(Select select, Project project, DataArea dataArea) {
    Filter rangeFilter = Filters.toFilter(dataArea.getKeyInterval());
    Filter filter = Filters.nullableAnd(rangeFilter, select.getFilter());
    return new DataTarget(filter, project.getPatterns(), project.getTagFilter());
  }

  public TaskExecuteResult executeQuery(
      DataUnit unit, DataTarget target, @Nullable AggregateType aggregateType) {
    try {
      RowStream stream = service.query(unit, target, aggregateType);
      return new TaskExecuteResult(stream);
    } catch (PhysicalException e) {
      return new TaskExecuteResult(e);
    }
  }
}
