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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class DenseImmutableFileFormat extends ImmutableFileFormat {

  public DenseImmutableFileFormat(String name, AbstractConfig config, CachePool cachePool) {
    super(name, config, cachePool);
  }

  protected abstract void flush(Path dst, Table.SubTable subTable) throws IOException, PhysicalException;

  protected abstract Table.Meta loadMeta(Path src) throws IOException;

  protected abstract RowStream scan(Path src, List<Field> fields, Filter predicate)
      throws IOException;

  @Override
  public void flush(Path dst, Table table) throws IOException, PhysicalException {
    Files.createDirectory(dst);
    List<Table.SubTable> subTableList = table.getSubTables();
    for (int i = 0; i < subTableList.size(); i++) {
      Path subTablePath = dst.resolve(String.format("subtable%010d", i) + "." + name);
      flush(subTablePath, subTableList.get(i));
    }
  }

  private final Pattern SUBTABLE_NAME_PATTERN = Pattern.compile("^subtable(\\d{10})\\.[^.]*$");

  @Override
  protected List<Table.SubTable> readSubTables(Path dir) throws IOException {
    List<Table.SubTable> subTables = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        Matcher matcher = SUBTABLE_NAME_PATTERN.matcher(fileName);
        if (matcher.matches()) {
          subTables.add(new DenseSubTable(path));
        }
      }
    } catch (NoSuchFileException e) {
      return subTables;
    }
    return subTables;
  }

  protected Table.Meta getOrLoadMeta(Path src) throws IOException {
    return (Table.Meta) getOrLoad(src, () -> loadMeta(src));
  }

  private class DenseSubTable implements Table.SubTable {

    private final Path path;

    private DenseSubTable(Path path) {
      this.path = Objects.requireNonNull(path);
    }

    @Override
    public String toString() {
      return "DenseSubTable{" +
              "path=" + path +
              '}';
    }

    @Override
    public Table.Meta getMeta() throws IOException {
      return getOrLoadMeta(path);
    }

    @Override
    public RowStream scan(List<Field> fields, Filter predicate) throws IOException {
      return DenseImmutableFileFormat.this.scan(path, fields, predicate);
    }
  }
}
