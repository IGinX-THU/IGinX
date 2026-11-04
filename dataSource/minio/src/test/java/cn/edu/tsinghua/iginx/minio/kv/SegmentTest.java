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
package cn.edu.tsinghua.iginx.minio.kv;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import org.junit.Test;

public class SegmentTest {

  @Test
  public void testRoundTripAllTypes() throws IOException {
    assertRoundTrip(DataType.BOOLEAN, true, false);
    assertRoundTrip(DataType.INTEGER, Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
    assertRoundTrip(DataType.LONG, Long.MIN_VALUE, 1L, Long.MAX_VALUE);
    assertRoundTrip(DataType.FLOAT, -1.5f, Float.NaN, 3.25f);
    assertRoundTrip(DataType.DOUBLE, -1.5d, Double.MAX_VALUE, 0d);
    assertRoundTrip(
        DataType.BINARY,
        "".getBytes(StandardCharsets.UTF_8),
        "中文 value".getBytes(StandardCharsets.UTF_8),
        new byte[] {0, -1, 127});
  }

  @Test
  public void testOfSortedMap() {
    TreeMap<Long, Object> data = new TreeMap<>();
    data.put(5L, 50L);
    data.put(-3L, -30L);
    data.put(10L, 100L);
    Segment segment = Segment.of(DataType.LONG, data);
    assertArrayEquals(new long[] {-3, 5, 10}, segment.getKeys());
    assertArrayEquals(new Object[] {-30L, 50L, 100L}, segment.getValues());
    assertEquals(-3, segment.minKey());
    assertEquals(10, segment.maxKey());
  }

  @Test(expected = IOException.class)
  public void testDecodeRejectsForeignObject() throws IOException {
    Segment.decode("not a segment".getBytes(StandardCharsets.UTF_8));
  }

  private static void assertRoundTrip(DataType type, Object... values) throws IOException {
    long[] keys = new long[values.length];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = i * 10L - 7;
    }
    Segment decoded = Segment.decode(new Segment(type, keys, values).encode());
    assertEquals(type, decoded.getType());
    assertArrayEquals(keys, decoded.getKeys());
    List<Object> expected = new ArrayList<>();
    List<Object> actual = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      expected.add(values[i] instanceof byte[] ? Arrays.toString((byte[]) values[i]) : values[i]);
      Object value = decoded.getValues()[i];
      actual.add(value instanceof byte[] ? Arrays.toString((byte[]) value) : value);
    }
    assertEquals(expected, actual);
  }
}
