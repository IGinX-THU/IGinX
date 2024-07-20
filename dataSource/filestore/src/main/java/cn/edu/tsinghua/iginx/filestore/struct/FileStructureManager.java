package cn.edu.tsinghua.iginx.filestore.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Collections;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class FileStructureManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStructureManager.class);

  private static final FileStructureManager INSTANCE = new FileStructureManager();

  public static FileStructureManager getInstance() {
    return INSTANCE;
  }

  private final ConcurrentHashMap<String, FileStructure> structures;

  private FileStructureManager() {
    this.structures = new ConcurrentHashMap<>();
    loadSpi(Thread.currentThread().getContextClassLoader());
  }

  private void loadSpi(ClassLoader classLoader) {
    ServiceLoader<FileStructure> serviceLoader = ServiceLoader.load(FileStructure.class, classLoader);
    for (FileStructure spi : serviceLoader) {
      LOGGER.debug("Discovered FileStructure {}", spi);
      FileStructure replaced = getInstance().register(spi);
      if (replaced != null) {
        LOGGER.warn("FileStructure {} is replaced by {} due to conflict name {}", replaced, spi, spi.getName());
      }
    }
  }

  public FileStructure register(FileStructure structure) {
    return structures.put(structure.getName(), structure);
  }

  public Collection<FileStructure> getAll() {
    return Collections.unmodifiableCollection(structures.values());
  }

  @Nullable
  public FileStructure getByName(String name) {
    return structures.get(name);
  }
}
