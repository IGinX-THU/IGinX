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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field.FieldIndex;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.SchemaClearEvent;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.SchemaFindEvent;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.SchemaInsertEvent;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.SchemaRemoveEvent;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Catalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(Catalog.class);
  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock();
  private final ReadWriteLock schemaLock = new ReentrantReadWriteLock();

  private final Map<Field, ColumnTableIndex> index = new HashMap<>();
  private final FieldIndex schema;

  public Catalog(CatalogConfig config) {
    this.schema = config.getSchemaIndexType().create();
  }

  public void verifyAndInsertFields(Set<Field> fields) throws TypeConflictedException {
    deleteLock.readLock().lock();
    try {
      schemaLock.readLock().lock();
      try {
        boolean allMatch = true;
        for (Field field : fields) {
          if (!schema.contain(field)) {
            allMatch = false;
            break;
          }
        }
        if (allMatch) {
          return;
        }
      } finally {
        schemaLock.readLock().unlock();
      }
      SchemaInsertEvent event = new SchemaInsertEvent();
      schemaLock.writeLock().lock();
      try {
        Set<Field> toInserted = new HashSet<>();
        for (Field field : fields) {
          if (!schema.contain(field)) {
            Field compactField =
                new Field(
                    field.getName().intern(),
                    field.getType(),
                    field.getTags().entrySet().stream()
                        .collect(
                            ImmutableMap.toImmutableMap(
                                e -> e.getKey().intern(), e -> e.getValue().intern())));
            toInserted.add(compactField);
          }
        }
        event.fieldsIndexed = index.size();
        event.fieldsInserted = toInserted.size();
        event.begin();
        schema.insert(toInserted);
        event.end();
        for (Field field : toInserted) {
          index.computeIfAbsent(field, f -> new ColumnTableIndex(f.getType()));
        }
      } finally {
        schemaLock.writeLock().unlock();
        event.commit();
      }
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  public Set<Field> findFields(List<String> patterns, @Nullable TagFilter tagFilter) {
    SchemaFindEvent event = new SchemaFindEvent();
    deleteLock.readLock().lock();
    schemaLock.readLock().lock();
    try {
      event.begin();
      Set<Field> result = schema.find(patterns, tagFilter);
      event.end();
      event.patternNum = patterns.size();
      event.fieldsIndexed = index.size();
      event.fieldsFound = result.size();
      return result;
    } finally {
      schemaLock.readLock().unlock();
      deleteLock.readLock().unlock();
      event.commit();
    }
  }

  public void addTable(long tableId, Table.Meta meta) throws TypeConflictedException {
    Map<Field, Table.Statistic> fieldStats = meta.getFieldStats();
    deleteLock.readLock().lock();
    try {
      verifyAndInsertFields(fieldStats.keySet());
      schemaLock.readLock().lock();
      try {
        for (Map.Entry<Field, Table.Statistic> entry : fieldStats.entrySet()) {
          Field field = entry.getKey();
          Table.Statistic statistic = entry.getValue();
          ColumnTableIndex columnIndex = index.get(field);
          columnIndex.addTable(tableId, statistic.getKeyRange());
        }
      } finally {
        schemaLock.readLock().unlock();
      }
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  public Set<Long> findTable(Set<Field> fields, RangeSet<Long> keyRangeSet) {
    Set<Long> result = new HashSet<>();
    deleteLock.readLock().lock();
    schemaLock.readLock().lock();
    try {
      for (Field field : fields) {
        ColumnTableIndex columnIndex = index.get(field);
        if (columnIndex == null) {
          continue;
        }
        Set<Long> tableIds = columnIndex.find(keyRangeSet);
        result.addAll(tableIds);
      }
    } finally {
      schemaLock.readLock().unlock();
      deleteLock.readLock().unlock();
    }
    return result;
  }

  public void delete(Set<Field> fields) throws TypeConflictedException {
    SchemaRemoveEvent event = new SchemaRemoveEvent();
    deleteLock.writeLock().lock();
    try {
      event.fieldsIndexed = index.size();
      event.fieldsRemoved = fields.size();
      event.begin();
      schema.remove(fields);
      event.end();
      for (Field field : fields) {
        index.remove(field);
      }
    } finally {
      deleteLock.writeLock().unlock();
      event.commit();
    }
  }

  public void delete(Set<Field> fields, RangeSet<Long> keyRangeSet) {
    deleteLock.writeLock().lock();
    try {
      for (Field field : fields) {
        ColumnTableIndex columnIndex = index.get(field);
        if (columnIndex == null) {
          continue;
        }
        columnIndex.delete(keyRangeSet);
      }
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  public void clear() {
    SchemaClearEvent event = new SchemaClearEvent();
    deleteLock.writeLock().lock();
    try {
      event.allocatedMemory = CachePool.bytesOf(schema);
      event.fieldsIndexed = index.size();
      event.begin();
      schema.clear();
      event.end();
      index.clear();
    } finally {
      deleteLock.writeLock().unlock();
      event.commit();
    }
  }
}
