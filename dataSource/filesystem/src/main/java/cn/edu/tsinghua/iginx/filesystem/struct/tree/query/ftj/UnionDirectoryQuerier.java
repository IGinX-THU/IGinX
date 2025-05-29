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
package cn.edu.tsinghua.iginx.filesystem.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filesystem.common.Closeables;
import cn.edu.tsinghua.iginx.filesystem.common.Strings;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.AbstractQuerier;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Querier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class UnionDirectoryQuerier extends AbstractQuerier {

  private final List<Querier> queriers;

  UnionDirectoryQuerier(
      Path path,
      String prefix,
      DataTarget target,
      List<Querier> subQueriers,
      ExecutorService executor)
      throws IOException {
    super(path, prefix, target, executor);
    queriers = Objects.requireNonNull(subQueriers);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append("&queriers=");
    for (Querier querier : queriers) {
      sb.append(Strings.shiftWithNewline(querier.toString()));
    }
    return sb.toString();
  }

  @Override
  public void close() throws IOException {
    Closeables.close(queriers);
    queriers.clear();
  }

  @Override
  public List<Future<RowStream>> query() {
    List<Future<RowStream>> rowStreams = new ArrayList<>();
    for (Querier querier : queriers) {
      rowStreams.addAll(querier.query());
    }
    return rowStreams;
  }
}
