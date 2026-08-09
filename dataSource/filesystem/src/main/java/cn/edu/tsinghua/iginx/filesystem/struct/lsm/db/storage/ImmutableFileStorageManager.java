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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.ImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.tombstone.Tombstone;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.tombstone.TombstoneStorage;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.tombstone.TombstoneTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.TableFlushEvent;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.RangeSet;
import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImmutableFileStorageManager implements StorageManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableFileStorageManager.class);

  private final Path dir;

  private final TombstoneStorage tombstoneStorage;

  private final ImmutableFileFormat immutableFileFormat;

  public ImmutableFileStorageManager(
      Path path, StorageConfig config, CachePool cachePool, Indexer indexer) {
    this.dir = path;
    this.tombstoneStorage = new TombstoneStorage(cachePool);
    this.immutableFileFormat =
        config.getFileFormat().create(config.getFileConfig(), cachePool, indexer);
  }

  @Override
  public String toString() {
    return dir.toString() + "(" + immutableFileFormat + ")";
  }

  private final Pattern TABLE_NAME_PATTERN = Pattern.compile("^table(\\d{19})$");

  @Override
  public Iterable<Long> list() throws IOException {
    List<Long> tableIds = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        Matcher matcher = TABLE_NAME_PATTERN.matcher(fileName);
        if (matcher.matches()) {
          String idStr = matcher.group(1);
          long tableId = Long.parseLong(idStr);
          tableIds.add(tableId);
        } else {
          throw new IOException("Invalid file name in directory " + dir + ": " + fileName);
        }
      }
    } catch (NoSuchFileException e) {
      LOGGER.debug("Directory {} does not exist, returning empty list", dir);
      return tableIds;
    }
    return tableIds;
  }

  @Override
  public void clear() throws IOException {
    PathUtils.delete(dir);
  }

  private Path getTableDir(long tableId) {
    String fileName = String.format("table%019d", tableId);
    return dir.resolve(fileName);
  }

  private Path getDataPath(Path tableDir) {
    return tableDir.resolve("data");
  }

  private Path getTombstonePath(Path tableDir) {
    return tableDir.resolve("tombstone");
  }

  @Override
  public void flush(long tableId, Table table) throws IOException, PhysicalException {
    Path tableDir = getTableDir(tableId);
    TableFlushEvent event = new TableFlushEvent();
    event.tableId = tableId;
    event.format = immutableFileFormat.toString();
    event.begin();
    try {
      try (AtomFlushPathWrapper wrapper = new AtomFlushPathWrapper(tableDir, false)) {
        Path dst = getDataPath(wrapper.getTmpPath());
        MoreFiles.createParentDirectories(dst);
        immutableFileFormat.flush(dst, table);
        wrapper.commit();
      }
      event.end();
      event.spaceUsed = PathUtils.sizeOf(tableDir);
    } finally {
      event.commit();
    }
  }

  @Override
  public Table read(long tableId) throws IOException {
    Path tableDir = getTableDir(tableId);
    Path src = getDataPath(tableDir);
    Table table = immutableFileFormat.read(src);
    Path tombstonePath = getTombstonePath(tableDir);
    Tombstone tombstone = tombstoneStorage.get(tombstonePath);
    if (tombstone != null) {
      table = new TombstoneTable(table, tombstone);
    }
    return table;
  }

  @Override
  public void delete(long tableId) throws IOException {
    Path tableDir = getTableDir(tableId);
    PathUtils.delete(tableDir);
  }

  @Override
  public void delete(long tableId, Set<Field> fields) throws IOException {
    Path tableDir = getTableDir(tableId);
    Path tombstonePath = getTombstonePath(tableDir);
    Tombstone tombstone = new Tombstone();
    fields.forEach(tombstone::add);
    tombstoneStorage.delete(tombstonePath, tombstone);
  }

  @Override
  public void delete(long tableId, Set<Field> fields, RangeSet<Long> keyRangeSet)
      throws IOException {
    Path tableDir = getTableDir(tableId);
    Path tombstonePath = getTombstonePath(tableDir);
    Tombstone tombstone = new Tombstone();
    fields.forEach(f -> tombstone.add(f, keyRangeSet));
    tombstoneStorage.delete(tombstonePath, tombstone);
  }
}
