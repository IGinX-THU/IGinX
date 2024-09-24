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
package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.DataUnits;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructureManager;
import cn.edu.tsinghua.iginx.filestore.struct.units.UnitsMerger;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageService implements Service {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageService.class);

  private static final String IGINX_DATA_PREFIX = "iginx_";

  private final StorageConfig dataConfig;
  private final StorageConfig dummyConfig;

  private final FileStructure dataStructure;
  private final FileStructure dummyStructure;

  private final Closeable dataShared;
  private final Closeable dummyShared;

  @GuardedBy("this")
  private final ConcurrentHashMap<DataUnit, FileManager> managers = new ConcurrentHashMap<>();

  private volatile boolean closed = false;

  public StorageService(@Nullable StorageConfig dataConfig, @Nullable StorageConfig dummyConfig)
      throws FileStoreException {
    this.dataConfig = dataConfig;
    this.dummyConfig = dummyConfig;

    LOGGER.debug("dataConfig: {}", dataConfig);
    LOGGER.debug("dummyConfig: {}", dummyConfig);

    this.dataStructure = getFileStructure(dataConfig);
    this.dummyStructure = getFileStructure(dummyConfig);

    LOGGER.debug("dataStructure: {}", dataStructure);
    LOGGER.debug("dummyStructure: {}", dummyStructure);

    this.dataShared = getShared(dataConfig, dataStructure);
    this.dummyShared = getShared(dummyConfig, dummyStructure);

    try {
      initManager();
    } catch (IOException e) {
      close();
      throw new FileStoreException("failed to initialize storage service", e);
    }
  }

  @Nullable
  private static FileStructure getFileStructure(@Nullable StorageConfig config)
      throws FileStoreException {
    if (config == null) {
      return null;
    }
    FileStructure structure = FileStructureManager.getInstance().getByName(config.getStruct());
    if (structure == null) {
      String message = String.format("unknown file structure %s", config.getStruct());
      throw new FileStoreException(message);
    }
    return structure;
  }

  @Nullable
  private static Closeable getShared(@Nullable StorageConfig config, FileStructure structure)
      throws FileStoreException {
    if (config == null) {
      return null;
    }
    try {
      return structure.newShared(config.getConfig());
    } catch (IOException e) {
      String message = String.format("failed to create shared for %s", config.getStruct());
      throw new FileStoreException(message, e);
    }
  }

  private void initManager() throws IOException {
    if (dataConfig != null) {
      for (String unitName : getUnitsIn(Paths.get(dataConfig.getRoot()))) {
        getOrCreateManager(DataUnits.of(false, unitName));
      }
    }

    if (dummyConfig != null) {
      if (dummyStructure.supportWrite()) {
        FileManager mergedDummyManager = new UnitsMerger(this::getNamedDummyUnits);
        managers.put(DataUnits.of(true, null), mergedDummyManager);
      } else {
        getOrCreateManager(DataUnits.of(true, null));
      }
    }
  }

  private TreeMap<DataUnit, FileManager> getNamedDummyUnits() throws IOException {
    for (String unitName : getUnitsIn(Paths.get(dummyConfig.getRoot()))) {
      getOrCreateManager(DataUnits.of(true, unitName));
    }
    TreeMap<DataUnit, FileManager> dummyManagers =
        new TreeMap<>(Comparator.comparing(DataUnit::getName));
    managers.forEach(
        (unit, manager) -> {
          if (unit.isDummy() && unit.getName() != null) {
            dummyManagers.put(unit, manager);
          }
        });
    return dummyManagers;
  }

  private static List<String> getUnitsIn(Path root) throws IOException {
    List<String> units = new ArrayList<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(
            root, path -> path.getFileName().toString().startsWith(IGINX_DATA_PREFIX))) {
      for (Path path : stream) {
        String unitNameWithPrefix = path.getFileName().toString();
        String unitName = unitNameWithPrefix.substring(IGINX_DATA_PREFIX.length());
        units.add(unitName);
      }
    } catch (NoSuchFileException e) {
      LOGGER.info("creating directory {} because it does not exist", root);
      Files.createDirectories(root);
    }
    return units;
  }

  private FileManager getOrCreateManager(DataUnit unit) throws IOException {
    if (closed) {
      throw new IllegalStateException("storage service is closed");
    }
    if (!managers.containsKey(unit))
      synchronized (this) {
        if (!managers.containsKey(unit)) {
          FileManager manager = createManager(unit);
          managers.put(unit, manager);
        }
      }
    return managers.get(unit);
  }

  private FileManager createManager(DataUnit unit) throws IOException {
    if (unit.isDummy()) {
      if (dummyConfig == null) {
        throw new IllegalStateException("dummy Unit data is requested but is not configured");
      }

      Path dummyRoot = Paths.get(dummyConfig.getRoot());
      if (unit.getName() != null) {
        dummyRoot = getPathOf(dummyRoot, unit.getName());
      }

      LOGGER.info("creating {} reader for {} in {}", dummyStructure, unit, dummyRoot);
      return dummyStructure.newReader(dummyRoot, dummyShared);
    } else {
      if (dataConfig == null) {
        throw new IllegalStateException("data Unit is requested but is not configured");
      }

      Path dataRoot = Paths.get(dataConfig.getRoot());
      Path dataUnitRoot = getPathOf(dataRoot, unit.getName());

      LOGGER.info("creating {} writer for {} in {}", dataStructure, unit, dataUnitRoot);
      return dataStructure.newWriter(dataUnitRoot, dataShared);
    }
  }

  private static Path getPathOf(Path root, String unitName) {
    if (unitName == null) {
      return root;
    } else {
      if (unitName.contains("..") || unitName.contains("/") || unitName.contains("\\")) {
        throw new IllegalArgumentException("Invalid unit name: " + unitName);
      }
      return root.resolve(IGINX_DATA_PREFIX + unitName);
    }
  }

  @Override
  public Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileStoreException {
    Map<DataUnit, DataBoundary> boundariesForEachUnit = new HashMap<>();
    for (DataUnit unit : managers.keySet()) {
      if (unit.isDummy() && unit.getName() != null) {
        continue;
      }
      DataBoundary boundary = getBoundary(unit, prefix);
      boundariesForEachUnit.put(unit, boundary);
    }
    return boundariesForEachUnit;
  }

  private static final DataBoundary DATA_BOUNDARY =
      new DataBoundary(Long.MIN_VALUE, Long.MAX_VALUE);
  private volatile DataBoundary dummyBoundary = null;
  private volatile String lastPrefix = null;

  // TODO: use lock and cache in better way
  private DataBoundary getBoundary(DataUnit unit, @Nullable String prefix)
      throws FileStoreException {
    if (unit.isDummy()) {
      try {
        if (dummyBoundary == null || !Objects.equals(prefix, lastPrefix))
          synchronized (this) {
            FileManager manager = getOrCreateManager(unit);
            dummyBoundary = manager.getBoundary(prefix);
            lastPrefix = prefix;
          }
        return dummyBoundary;
      } catch (IOException e) {
        String message =
            String.format("failed to get boundary for unit %s with prefix %s", unit, prefix);
        throw new FileStoreException(message, e);
      }
    } else {
      return DATA_BOUNDARY;
    }
  }

  @Override
  public RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate)
      throws FileStoreException {
    try {
      FileManager manager = getOrCreateManager(unit);
      return manager.query(target, aggregate);
    } catch (IOException e) {
      String msg;
      if (LOGGER.isDebugEnabled()) {
        msg =
            String.format(
                "failed to query data from %s with target %s and aggregate %s",
                unit, target, aggregate);
      } else {
        msg = "Failed to query data";
      }
      throw new FileStoreException(msg, e);
    }
  }

  // TODO: close and remove manager after clear all data
  @Override
  public void delete(DataUnit unit, DataTarget target) throws FileStoreException {
    if (unit.isDummy()) {
      throw new IllegalStateException("cannot delete data from dummy unit");
    }
    try {
      FileManager manager = getOrCreateManager(unit);
      manager.delete(target);
    } catch (IOException e) {
      String msg;
      if (LOGGER.isDebugEnabled()) {
        msg = String.format("failed to delete unit %s with target %s", unit, target);
      } else {
        msg = "failed to delete data";
      }
      throw new FileStoreException(msg, e);
    }
  }

  @Override
  public void insert(DataUnit unit, DataView dataView) throws FileStoreException {
    if (unit.isDummy()) {
      throw new IllegalStateException("cannot insert data into dummy unit");
    }
    try {
      FileManager manager = getOrCreateManager(unit);
      manager.insert(dataView);
    } catch (IOException e) {
      String msg;
      if (LOGGER.isDebugEnabled()) {
        msg = String.format("failed to insert data to %s", unit);
      } else {
        msg = "failed to insert data";
      }
      throw new FileStoreException(msg, e);
    }
  }

  // TODO: use rwlock to make sure close is exclusive with other operations
  @Override
  public synchronized void close() throws FileStoreException {
    if (closed) {
      return;
    }
    closed = true;
    FileStoreException exception = new FileStoreException("Failed to close storage service");
    for (FileManager manager : managers.values()) {
      try {
        manager.close();
      } catch (IOException e) {
        exception.addSuppressed(e);
      }
    }
    managers.clear();
    if (dataShared != null) {
      try {
        dataShared.close();
      } catch (IOException e) {
        exception.addSuppressed(e);
      }
    }
    if (dummyShared != null) {
      try {
        dummyShared.close();
      } catch (IOException e) {
        exception.addSuppressed(e);
      }
    }
    if (exception.getSuppressed().length > 0) {
      throw exception;
    }
  }
}
