package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructure;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructureManager;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageService implements Service {

  private final static Logger LOGGER = LoggerFactory.getLogger(StorageService.class);

  private final static String IGINX_DATA_PREFIX = "iginx_";

  private final StorageConfig dataConfig;
  private final StorageConfig dummyConfig;

  private final FileStructure dataStructure;
  private final FileStructure dummyStructure;

  @GuardedBy("this")
  private final HashMap<DataUnit, FileManager> managers = new HashMap<>();
  private volatile boolean closed = false;

  public StorageService(@Nullable StorageConfig dataConfig, @Nullable StorageConfig dummyConfig) throws FileStoreException {
    this.dataConfig = dataConfig;
    this.dummyConfig = dummyConfig;

    this.dataStructure = getFileStructure(dataConfig);
    this.dummyStructure = getFileStructure(dummyConfig);

    try {
      initManager();
    } catch (IOException e) {
      throw new FileStoreException("Failed to initialize storage service", e);
    }
  }

  @Nullable
  private FileStructure getFileStructure(@Nullable StorageConfig config) throws FileStoreException {
    if (config == null) {
      return null;
    }
    FileStructure structure = FileStructureManager.getInstance().getByName(config.getType());
    if (structure == null) {
      String message = String.format("Not found file structure: %s", config.getType());
      throw new FileStoreException(message);
    }
    return structure;
  }

  private void initManager() throws IOException {
    if (dataConfig != null) {
      for (String unitName : getUnitsIn(Paths.get(dataConfig.getRoot()))) {
        getOrCreateManager(new DataUnit(false, unitName));
      }
    }

    if (dummyConfig != null) {
      getOrCreateManager(new DataUnit(true, null));
    }
  }

  private static List<String> getUnitsIn(Path root) throws IOException {
    List<String> units = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(root, path -> path.getFileName().toString().startsWith(IGINX_DATA_PREFIX))) {
      for (Path path : stream) {
        String unitNameWithPrefix = path.getFileName().toString();
        String unitName = unitNameWithPrefix.substring(IGINX_DATA_PREFIX.length());
        units.add(unitName);
      }
    }
    return units;
  }

  private synchronized FileManager getOrCreateManager(DataUnit unit) throws IOException {
    if (closed) {
      throw new IllegalStateException("Storage service is closed");
    }
    if (!managers.containsKey(unit)) {
      FileManager manager = createManager(unit);
      managers.put(unit, manager);
    }
    return managers.get(unit);
  }

  private FileManager createManager(DataUnit unit) throws IOException {
    if (unit.isDummy()) {
      if (dummyConfig == null) {
        throw new IllegalStateException("Dummy Unit data is requested but is not configured");
      }
      if (unit.getName() != null) {
        throw new IllegalStateException("Dummy Unit data is requested but name is not null");
      }

      Path dummyRoot = Paths.get(dummyConfig.getRoot());

      LOGGER.info("Creating {} reader for {} in {}", dummyStructure, unit, dummyRoot);
      return dummyStructure.newReader(dummyRoot, dummyConfig.getConfig());
    } else {
      if (dataConfig == null) {
        throw new IllegalStateException("Data Unit is requested but is not configured");
      }

      Path dataRoot = Paths.get(dataConfig.getRoot());
      Path dataUnitRoot = getPathOf(dataRoot, unit.getName());

      LOGGER.info("Creating {} writer for {} in {}", dummyStructure, unit, dataUnitRoot);
      return dataStructure.newWriter(dataUnitRoot, dataConfig.getConfig());
    }
  }

  private static Path getPathOf(Path root, String unitName) {
    if (unitName == null) {
      return root;
    } else {
      return root.resolve(IGINX_DATA_PREFIX + unitName);
    }
  }

  @Override
  public synchronized Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileStoreException {
    Map<DataUnit, DataBoundary> boundariesForEachUnit = new HashMap<>();
    for (DataUnit unit : managers.keySet()) {
      DataBoundary boundary = getBoundary(unit, prefix);
      boundariesForEachUnit.put(unit, boundary);
    }
    return boundariesForEachUnit;
  }

  private DataBoundary getBoundary(DataUnit unit, @Nullable String prefix) throws FileStoreException {
    if (unit.isDummy()) {
      try {
        FileManager manager = getOrCreateManager(unit);
        return manager.getBoundary(prefix);
      } catch (IOException e) {
        String message = String.format("Failed to get boundary for unit %s with prefix %s", unit, prefix);
        throw new FileStoreException(message, e);
      }
    } else {
      return new DataBoundary(Long.MIN_VALUE, Long.MAX_VALUE);
    }
  }

  @Override
  public RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate) throws FileStoreException {
    try {
      FileManager manager = getOrCreateManager(unit);
      return manager.query(target, aggregate);
    } catch (IOException e) {
      String msg;
      if (LOGGER.isDebugEnabled()) {
        msg = String.format("Failed to query data from %s with target %s and aggregate %s", unit, target, aggregate);
      } else {
        msg = "Failed to query data";
      }
      throw new FileStoreException(msg, e);
    }
  }

  @Override
  public void delete(DataUnit unit, DataTarget target) throws FileStoreException {
    if (unit.isDummy()) {
      throw new IllegalStateException("Cannot delete data from dummy unit");
    }
    try {
      FileManager manager = getOrCreateManager(unit);
      manager.delete(target);
    } catch (IOException e) {
      String msg;
      if (LOGGER.isDebugEnabled()) {
        msg = String.format("Failed to delete unit %s with target %s", unit, target);
      } else {
        msg = "Failed to delete data";
      }
      throw new FileStoreException(msg, e);
    }
  }

  @Override
  public void insert(DataUnit unit, DataView dataView) throws FileStoreException {
    if (unit.isDummy()) {
      throw new IllegalStateException("Cannot insert data into dummy unit");
    }
    try {
      FileManager manager = getOrCreateManager(unit);
      manager.insert(dataView);
    } catch (IOException e) {
      String msg;
      if (LOGGER.isDebugEnabled()) {
        msg = String.format("Failed to insert data to %s", unit);
      } else {
        msg = "Failed to insert data";
      }
      throw new FileStoreException(msg, e);
    }
  }

  @Override
  public synchronized void close() throws FileStoreException {
    if (closed) {
      return;
    }
    closed = true;
    FileStoreException exception = null;
    for (FileManager manager : managers.values()) {
      try {
        manager.close();
      } catch (IOException e) {
        if (exception == null) {
          exception = new FileStoreException("Failed to close storage service", e);
        } else {
          exception.addSuppressed(e);
        }
      }
      managers.clear();
    }
  }
}
