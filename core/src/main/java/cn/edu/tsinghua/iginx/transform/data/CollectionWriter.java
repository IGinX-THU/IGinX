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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import java.util.List;

public class CollectionWriter extends ExportWriter {

  private BatchData collectedData;

  @Override
  public void write(BatchData batchData) throws WriteBatchException {
    Header header = batchData.getHeader();
    if (collectedData == null) {
      collectedData = new BatchData(header);
    } else {
      checkColumns(collectedData.getHeader(), header);
    }
    if (batchData.getRowList().isEmpty()) {
      return;
    }
    if (collectedData.getRowList().isEmpty()) {
      // types of an empty batch from python are placeholders, adopt the real ones
      collectedData = new BatchData(header);
    }
    for (Row row : batchData.getRowList()) {
      collectedData.appendRow(row);
    }
  }

  private static void checkColumns(Header expected, Header actual) throws WriteBatchException {
    List<Field> expectedFields = expected.getFields();
    List<Field> actualFields = actual.getFields();
    boolean same =
        expected.hasKey() == actual.hasKey() && expectedFields.size() == actualFields.size();
    for (int i = 0; same && i < expectedFields.size(); i++) {
      same = expectedFields.get(i).getFullName().equals(actualFields.get(i).getFullName());
    }
    if (!same) {
      throw new WriteBatchException(
          "Inconsistent columns in one result, expected: " + expected + ", got: " + actual);
    }
  }

  /** Hands over the complete result and clears the reference held by this collector. */
  public BatchData takeCollectedData() {
    if (collectedData == null) {
      throw new IllegalStateException("No data has been collected.");
    }
    BatchData batchData = collectedData;
    collectedData = null;
    return batchData;
  }

  @Override
  public void reset() {
    collectedData = null;
  }
}
