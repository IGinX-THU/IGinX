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
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public abstract class SparseImmutableFileFormat extends ImmutableFileFormat {

  public SparseImmutableFileFormat(String name, AbstractConfig config, CachePool cachePool) {
    super(name, config, cachePool);
  }

  protected abstract void flush(Path dstWithSuffix, List<Table.SubTable> subTables) throws IOException, PhysicalException;

  protected abstract Object loadFileMeta(Path srcWithSuffix) throws IOException;

  protected abstract List<String> readSubTableNames(Path srcWithSuffix) throws IOException;

  protected abstract Table.Meta loadMeta(Path srcWithSuffix, String subTableName) throws IOException;

  protected abstract RowStream scan(Path srcWithSuffix, String subTableName, List<Field> fields, Filter predicate)
      throws IOException;

  @Override
  public void flush(Path dst, Table table) throws IOException, PhysicalException  {
    Path dstWithSuffix = getWithSuffixPath(dst);
    flush(dstWithSuffix, table.getSubTables());
  }

  @Override
  protected List<Table.SubTable> readSubTables(Path src) throws IOException {
    Path srcWithSuffix = getWithSuffixPath(src);
    List<String> subTableNames = readSubTableNames(srcWithSuffix);
    return subTableNames.stream().map(n -> new DenseSubTable(srcWithSuffix, n)).collect(ImmutableList.toImmutableList());
  }

  protected Object getOrLoadFileMeta(Path srcWithSuffix) throws IOException {
    return getOrLoad(srcWithSuffix, () -> loadFileMeta(srcWithSuffix));
  }

  protected Path getWithSuffixPath(Path src) {
    return src.resolveSibling(src.getFileName() + "." + name);
  }

  private class DenseSubTable implements Table.SubTable {

    private final Path srcWithSuffix;
    private final String subTableName;

    private DenseSubTable(Path srcWithSuffix, String subTableName) {
      this.srcWithSuffix = Objects.requireNonNull(srcWithSuffix);
      this.subTableName = Objects.requireNonNull(subTableName);
    }

    @Override
    public String toString() {
      return "DenseSubTable{" +
              "srcWithSuffix=" + srcWithSuffix +
              ", subTableName='" + subTableName + '\'' +
              '}';
    }

    @Override
    public Table.Meta getMeta() throws IOException {
      return loadMeta(srcWithSuffix, subTableName);
    }

    @Override
    public RowStream scan(List<Field> fields, Filter predicate) throws IOException {
      return SparseImmutableFileFormat.this.scan(srcWithSuffix, subTableName, fields, predicate);
    }
  }
}
