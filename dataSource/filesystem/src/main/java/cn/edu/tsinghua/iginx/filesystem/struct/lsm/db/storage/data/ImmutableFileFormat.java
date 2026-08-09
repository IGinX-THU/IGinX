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
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AbstractTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class ImmutableFileFormat {

  protected final String name;
  protected final AbstractConfig config;
  protected final CachePool cachePool;

  public ImmutableFileFormat(String name, AbstractConfig config, CachePool cachePool) {
    this.name = Objects.requireNonNull(name);
    this.config = Objects.requireNonNull(config);
    this.cachePool = Objects.requireNonNull(cachePool);
    List<AbstractConfig.ValidationProblem> problems = config.validate();
    if (!problems.isEmpty()) {
      throw new IllegalArgumentException("invalid config for format " + name + ": " + problems);
    }
  }

  @Override
  public String toString() {
    return "ImmutableFileFormat{" + "name='" + name + '\'' + ", config=" + config + '}';
  }

  public abstract void flush(Path dst, Table table) throws IOException, PhysicalException;

  protected abstract List<Table.SubTable> readSubTables(Path src) throws IOException;

  public Table read(Path src) throws IOException {
    return new ImmutableFileFormatTable(src);
  }

  protected Object getOrLoad(Object key, Loader loader) throws IOException {
    Object cached = cachePool.asMap().get(key);
    if (cached != null) {
      return cached;
    }
    Object loaded = loader.load();
    cachePool.asMap().put(key, loaded);
    return loaded;
  }

  protected interface Loader {
    Object load() throws IOException;
  }

  protected RowStream scanAll(Table.SubTable subTable) throws IOException {
    List<Field> fields = new ArrayList<>(subTable.getMeta().getFieldStats().keySet());
    return subTable.scan(fields, new BoolFilter(true));
  }

  private class ImmutableFileFormatTable extends AbstractTable {

    private final Path path;

    private ImmutableFileFormatTable(Path path) {
      this.path = Objects.requireNonNull(path);
    }

    @Override
    public List<SubTable> getSubTables() throws IOException {
      return readSubTables(path);
    }

    @Override
    public String toString() {
      return "ImmutableFileFormatTable{"
          + "path="
          + path
          + ","
          + "format="
          + ImmutableFileFormat.this.name
          + '}';
    }
  }
}
