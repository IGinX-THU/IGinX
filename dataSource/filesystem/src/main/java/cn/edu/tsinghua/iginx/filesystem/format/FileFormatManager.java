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
package cn.edu.tsinghua.iginx.filesystem.format;

import java.util.Collection;
import java.util.Collections;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    if (format.getName().contains(".")) {
      LOGGER.warn("FileFormat name {} contains dot, ignored", format.getName());
      return null;
    }
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
