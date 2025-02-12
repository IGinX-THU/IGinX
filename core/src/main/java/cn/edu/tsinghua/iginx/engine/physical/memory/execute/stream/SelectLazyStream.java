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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import java.util.ArrayList;
import java.util.List;

public class SelectLazyStream extends UnaryLazyStream {

  private final Select select;

  private static final int BATCH_SIZE = 1000;

  private int cacheIndex = 0;

  private List<Row> nextBatchCache = new ArrayList<>();

  public SelectLazyStream(Select select, RowStream stream) {
    super(stream);
    this.select = select;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (cacheIndex >= nextBatchCache.size()) {
      calculateNextBatch();
    }
    return cacheIndex < nextBatchCache.size();
  }

  private void calculateNextBatch() throws PhysicalException {
    int rowCnt = 0;
    List<Row> rows = new ArrayList<>();
    while (stream.hasNext() && rowCnt < BATCH_SIZE) {
      rows.add(stream.next());
      rowCnt++;
    }
    nextBatchCache = RowUtils.cacheFilterResult(rows, select.getFilter());
    cacheIndex = 0;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return nextBatchCache.get(cacheIndex++);
  }
}
