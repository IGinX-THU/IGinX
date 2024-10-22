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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.AreaFilterScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.util.iterator.Scanner;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

public class DeletedTable implements Table {

  private final Table table;

  private final AreaSet<Long, String> deleted;

  public DeletedTable(Table table, AreaSet<Long, String> deleted) {
    this.table = table;
    this.deleted = deleted;
  }

  @Override
  public TableMeta getMeta() throws IOException {
    return new DeletedTableMeta(table.getMeta(), deleted);
  }

  @Override
  public Scanner<Long, Scanner<String, Object>> scan(
      Set<String> fields, RangeSet<Long> range, @Nullable Filter superSetPredicate)
      throws IOException {
    return new AreaFilterScanner<>(table.scan(fields, range, superSetPredicate), deleted);
  }
}
