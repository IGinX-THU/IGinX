/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.Catalog;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.ImmutableFileStorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.*;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);

  private final Catalog catalog;
  private final StorageManager storageManager;

  public TableStorage(Path path, StorageConfig storage, Catalog catalog, CachePool cachePool, Indexer indexer) {
    this.catalog = catalog;
    this.storageManager = new ImmutableFileStorageManager(path, storage, cachePool, indexer);

    try {
      for (long tableId : storageManager.list()) {
        LOGGER.debug("reloading table: {}", tableId);
        Table table = storageManager.read(tableId);
        catalog.addTable(tableId, table.getMeta());
      }
    } catch (IOException | TypeConflictedException e) {
      throw new StorageRuntimeException("reload failed: " + path, e);
    }
  }

  public void flush(long id, Table table, boolean commit) throws IOException, PhysicalException {
    storageManager.flush(id, table);
    if (commit) {
      catalog.addTable(id, table.getMeta());
    }
  }

  public void deleteUncommitted(long tableId) throws IOException {
    storageManager.delete(tableId);
  }

  public void delete(Set<Field> fields) throws IOException {
    Set<Long> tableIds = catalog.findTable(fields, ImmutableRangeSet.of(Range.all()));
    for (long tableId : tableIds) {
      storageManager.delete(tableId, fields);
    }
  }

  public void delete(Set<Field> fields, RangeSet<Long> keyRangeSet) throws IOException {
    Set<Long> tableIds = catalog.findTable(fields, keyRangeSet);
    for (long tableId : tableIds) {
      storageManager.delete(tableId, fields, keyRangeSet);
    }
  }

  public List<Table> load(List<Field> fields, RangeSet<Long> keyRangeSet) throws IOException {
    Set<Long> tableIds = catalog.findTable(ImmutableSet.copyOf(fields), keyRangeSet);
    List<Long> sortedTableIds = new ArrayList<>(tableIds);
    sortedTableIds.sort(Comparator.naturalOrder());

    List<Table> tables = new ArrayList<>();
    for (long tableId : sortedTableIds) {
      tables.add(storageManager.read(tableId));
    }
    return tables;
  }

  public void clear() {
    try {
      storageManager.clear();
    } catch (NoSuchFileException ignored) {
    } catch (IOException e) {
      LOGGER.error("clear failed", e);
    }
  }
}
