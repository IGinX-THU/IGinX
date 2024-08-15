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
package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filestore.common.Closeables;
import cn.edu.tsinghua.iginx.filestore.common.Strings;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.AbstractQuerier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class TreeJoinQuerier extends AbstractQuerier {

  private final List<Querier> queriers = new ArrayList<>();

  TreeJoinQuerier(Path path, String prefix, DataTarget target) {
    super(path, prefix, target);
  }

  public void add(Querier querier) {
    queriers.add(querier);
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
  public List<RowStream> query() throws IOException {
    List<RowStream> rowStreams = new ArrayList<>();
    for (Querier querier : queriers) {
      rowStreams.addAll(querier.query());
    }
    return rowStreams;
  }
}
