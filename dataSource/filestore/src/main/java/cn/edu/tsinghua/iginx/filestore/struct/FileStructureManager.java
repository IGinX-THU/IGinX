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
package cn.edu.tsinghua.iginx.filestore.struct;

import java.util.Collection;
import java.util.Collections;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    loadSpi();
  }

  private void loadSpi() {
    ServiceLoader<FileStructure> serviceLoader = ServiceLoader.load(FileStructure.class);
    for (FileStructure spi : serviceLoader) {
      LOGGER.debug("Discovered FileStructure {}", spi);
      FileStructure replaced = register(spi);
      if (replaced != null) {
        LOGGER.warn(
            "FileStructure {} is replaced by {} due to conflict name {}",
            replaced,
            spi,
            spi.getName());
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
