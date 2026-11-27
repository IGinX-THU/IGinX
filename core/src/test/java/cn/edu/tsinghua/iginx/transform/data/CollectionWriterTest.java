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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import java.util.Collections;
import org.junit.Test;

public class CollectionWriterTest {

  private static Header header(String name, DataType type) {
    return new Header(Collections.singletonList(new Field(name, type)));
  }

  private static BatchData batch(Header header, Object... values) {
    BatchData batchData = new BatchData(header);
    for (Object value : values) {
      batchData.appendRow(new Row(header, new Object[] {value}));
    }
    return batchData;
  }

  @Test
  public void testEmptyBatchesMergeIntoEmptyTable() throws WriteBatchException {
    CollectionWriter writer = new CollectionWriter();
    writer.write(batch(header("x", DataType.BINARY)));
    writer.write(batch(header("x", DataType.BINARY)));
    BatchData collected = writer.takeCollectedData();
    assertEquals("x", collected.getHeader().getField(0).getFullName());
    assertTrue(collected.getRowList().isEmpty());
  }

  @Test
  public void testAdoptTypesOfFirstNonEmptyBatch() throws WriteBatchException {
    CollectionWriter writer = new CollectionWriter();
    writer.write(batch(header("x", DataType.BINARY)));
    writer.write(batch(header("x", DataType.LONG), 1L, 2L));
    writer.write(batch(header("x", DataType.BINARY)));
    BatchData collected = writer.takeCollectedData();
    assertEquals(DataType.LONG, collected.getHeader().getField(0).getType());
    assertEquals(2, collected.getRowList().size());
  }

  @Test
  public void testColumnsOfLaterNonEmptyBatchMayHaveOtherTypes() throws WriteBatchException {
    // a column of None values is typed BINARY by PemjaReader
    CollectionWriter writer = new CollectionWriter();
    writer.write(batch(header("x", DataType.BINARY), (Object) null));
    writer.write(batch(header("x", DataType.LONG), 1L));
    assertEquals(2, writer.takeCollectedData().getRowList().size());
  }

  @Test
  public void testInconsistentColumns() throws WriteBatchException {
    CollectionWriter writer = new CollectionWriter();
    writer.write(batch(header("x", DataType.LONG)));
    assertThrows(
        WriteBatchException.class, () -> writer.write(batch(header("y", DataType.LONG), 1L)));
  }

  @Test
  public void testTakeCollectedData() throws WriteBatchException {
    CollectionWriter writer = new CollectionWriter();
    assertThrows(IllegalStateException.class, writer::takeCollectedData);
    writer.write(batch(header("x", DataType.LONG), 1L));
    BatchData collected = writer.takeCollectedData();
    assertEquals(1, collected.getRowList().size());
    assertThrows(IllegalStateException.class, writer::takeCollectedData);
  }
}
