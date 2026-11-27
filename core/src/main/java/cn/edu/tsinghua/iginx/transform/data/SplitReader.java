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
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.util.List;

public class SplitReader extends AbstractBatchReader {

  private final List<Row> rowList;

  private final Header header;

  private int offset = 0;

  public SplitReader(BatchData batchData, int batchSize) {
    super(batchSize);
    this.rowList = batchData.getRowList();
    this.header = batchData.getHeader();
  }

  @Override
  protected Header readHeader() {
    return header;
  }

  @Override
  protected boolean hasNextRow() {
    return offset < rowList.size();
  }

  @Override
  protected Row nextRow() {
    return rowList.get(offset++);
  }

  @Override
  public void close() {
    rowList.clear();
  }
}
