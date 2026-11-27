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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Test;

public class AbstractBatchReaderTest {

  private static final Header HEADER =
      new Header(Field.KEY, Collections.singletonList(new Field("x", DataType.LONG)));

  private static BatchData table(Header header, int rowCount) {
    BatchData batchData = new BatchData(header);
    for (int i = 0; i < rowCount; i++) {
      batchData.appendRow(new Row(header, i, new Object[] {(long) i}));
    }
    return batchData;
  }

  private static List<Integer> batchSizes(SplitReader reader) throws ReadBatchException {
    List<Integer> sizes = new ArrayList<>();
    while (reader.hasNextBatch()) {
      sizes.add(reader.loadNextBatch().getRowList().size());
    }
    return sizes;
  }

  @Test
  public void testSplitByBatchSize() throws ReadBatchException {
    assertEquals(Arrays.asList(50, 50, 20), batchSizes(new SplitReader(table(HEADER, 120), 50)));
  }

  @Test
  public void testNoTrailingEmptyBatchWhenRowsFillLastBatch() throws ReadBatchException {
    assertEquals(Arrays.asList(50, 50), batchSizes(new SplitReader(table(HEADER, 100), 50)));
  }

  @Test
  public void testEmptyResultDeliversOneEmptyBatch() throws ReadBatchException {
    SplitReader reader = new SplitReader(table(HEADER, 0), 50);
    assertTrue(reader.hasNextBatch());
    assertTrue(reader.hasNextBatch());
    BatchData batchData = reader.loadNextBatch();
    assertEquals(HEADER, batchData.getHeader());
    assertTrue(batchData.getRowList().isEmpty());
    assertFalse(reader.hasNextBatch());
    assertFalse(reader.hasNextBatch());
    assertThrows(NoSuchElementException.class, reader::loadNextBatch);
  }

  @Test
  public void testRepeatedCheckDoesNotSkipBatch() throws ReadBatchException {
    SplitReader reader = new SplitReader(table(HEADER, 3), 2);
    assertTrue(reader.hasNextBatch());
    assertTrue(reader.hasNextBatch());
    assertEquals(2, reader.loadNextBatch().getRowList().size());
    assertTrue(reader.hasNextBatch());
    assertEquals(1, reader.loadNextBatch().getRowList().size());
    assertFalse(reader.hasNextBatch());
  }

  @Test
  public void testNonPositiveBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> new SplitReader(table(HEADER, 1), 0));
    assertThrows(IllegalArgumentException.class, () -> new SplitReader(table(HEADER, 1), -1));
  }

  @Test
  public void testHeaderWithoutColumn() {
    SplitReader reader = new SplitReader(table(Header.EMPTY_HEADER, 0), 50);
    assertThrows(ReadBatchException.class, reader::hasNextBatch);
  }

  @Test
  public void testKeyOnlyHeaderIsValid() throws ReadBatchException {
    assertEquals(
        Collections.singletonList(0),
        batchSizes(new SplitReader(table(Header.EMPTY_HEADER_WITH_KEY, 0), 50)));
  }
}
