package cn.edu.tsinghua.iginx.filestore.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Collections;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class FileFormatManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileFormatManager.class);

  private static final FileFormatManager INSTANCE = new FileFormatManager();

  public static FileFormatManager getInstance() {
    return INSTANCE;
  }

  private final ConcurrentHashMap<String, FileFormat> formats;

  private final ConcurrentHashMap<String, String> extensionToFormat;

  private FileFormatManager() {
    this.formats = new ConcurrentHashMap<>();
    this.extensionToFormat = new ConcurrentHashMap<>();
    loadSpi(FileFormatManager.class.getClassLoader());
    rebuildIndex();
  }

  public void loadSpi(ClassLoader loader) {
    ServiceLoader<FileFormat> serviceLoader = ServiceLoader.load(FileFormat.class, loader);
    for (FileFormat spi : serviceLoader) {
      LOGGER.debug("Discovered FileFormat {}", spi);
      FileFormat replaced = register(spi);
      if (replaced != null) {
        LOGGER.warn(
            "FileFormat {} is replaced by {} due to conflict name {}",
            replaced,
            spi,
            spi.getName());
      }
    }
  }

  public void rebuildIndex() {
    extensionToFormat.clear();
    for (FileFormat format : formats.values()) {
      for (String extension : format.getExtensions()) {
        String old = extensionToFormat.put(extension, format.getName());
        if (old != null) {
          LOGGER.warn(
              "Index of {} is replaced by {} due to conflict extension {}",
              old,
              format.getName(),
              extension);
        }
      }
    }
  }

  public FileFormat register(FileFormat format) {
    return formats.put(format.getName(), format);
  }

  public Collection<FileFormat> getAll() {
    return Collections.unmodifiableCollection(formats.values());
  }

  @Nullable
  public FileFormat getByName(@Nullable String name) {
    if (name == null) {
      return null;
    }
    return formats.get(name);
  }

  @Nullable
  public FileFormat getByExtension(@Nullable String extension) {
    if (extension == null) {
      return null;
    }
    return getByName(extensionToFormat.get(extension));
  }
  
  public FileFormat getByExtension(@Nullable String extension, FileFormat defaultFormat) {
    FileFormat format = getByExtension(extension);
    return format == null ? defaultFormat : format;
  }
}
