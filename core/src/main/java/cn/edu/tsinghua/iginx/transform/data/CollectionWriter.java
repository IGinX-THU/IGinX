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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

public class CollectionWriter extends ExportWriter {

  private BatchData collectedData;

  @Override
  public void write(BatchData batchData) {
    if (collectedData == null) {
      collectedData = new BatchData(batchData.getHeader());
    }
    for (Row row : batchData.getRowList()) {
      collectedData.appendRow(row);
    }
  }

  public BatchData getCollectedData() {
    return collectedData;
  }

  @Override
  public void reset() {
    collectedData = null;
  }
}
