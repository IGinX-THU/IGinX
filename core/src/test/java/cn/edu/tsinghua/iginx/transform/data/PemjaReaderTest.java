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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class PemjaReaderTest {

  private static List<Object> list(Object... values) {
    return new ArrayList<>(Arrays.asList(values));
  }

  private static BatchData readOnlyBatch(Object data) throws ReadBatchException {
    PemjaReader reader = new PemjaReader(data, 50);
    assertTrue(reader.hasNextBatch());
    BatchData batchData = reader.loadNextBatch();
    assertFalse(reader.hasNextBatch());
    return batchData;
  }

  private static void assertInvalid(Object data) {
    assertThrows(ReadBatchException.class, () -> readOnlyBatch(data));
  }

  @Test
  public void testHeaderOnly() throws ReadBatchException {
    BatchData headerOnly = readOnlyBatch(list(list("y")));
    assertEquals("y", headerOnly.getHeader().getField(0).getFullName());
    assertTrue(headerOnly.getRowList().isEmpty());
  }

  @Test
  public void testMissingOrEmptyHeader() {
    assertInvalid(null);
    assertInvalid(list());
    assertInvalid(list(list()));
    assertInvalid(list(list("")));
    assertInvalid(list(list("x", 1)));
    assertInvalid(list("y"));
    assertInvalid(list("x", "y"));
    assertInvalid(123L);
    assertInvalid("y");
    assertInvalid(Collections.singletonMap("y", 1L));
    assertInvalid(list(list(), list(1L)));
  }

  @Test
  public void testNullValues() throws ReadBatchException {
    BatchData allNull = readOnlyBatch(list(list("x"), list((Object) null)));
    assertEquals(1, allNull.getRowList().size());
    assertEquals(DataType.BINARY, allNull.getHeader().getField(0).getType());

    BatchData sparse = readOnlyBatch(list(list("x", "y"), list(null, 1L), list(2L, 3L)));
    assertEquals(2, sparse.getRowList().size());
    assertEquals(Arrays.asList(DataType.LONG, DataType.LONG), sparse.getHeader().getDataTypes());
  }

  @Test
  public void testColumnTypes() throws ReadBatchException {
    Header header =
        readOnlyBatch(list(list("a", "b", "c", "d"), list(1L, 1.5, true, "s"))).getHeader();
    assertEquals(
        Arrays.asList(DataType.LONG, DataType.DOUBLE, DataType.BOOLEAN, DataType.BINARY),
        header.getDataTypes());
    assertInvalid(list(list("x"), list(Collections.emptyMap())));
  }

  @Test
  public void testMalformedRows() {
    assertInvalid(list(list("x"), list()));
    assertInvalid(list(list("x", "y"), list(1L)));
    assertInvalid(list(list("x"), list(1L, 2L)));
    assertInvalid(list(list("x"), list(1L), list(1L, 2L)));
    assertInvalid(list(list("x"), "abc"));
    assertInvalid(list(list("x"), null));
  }
}
