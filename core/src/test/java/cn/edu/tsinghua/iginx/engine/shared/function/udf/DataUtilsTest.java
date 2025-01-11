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
package cn.edu.tsinghua.iginx.engine.shared.function.udf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.DataUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class DataUtilsTest {

  private Pair<Table, List<List<Object>>> generateTable() {
    Header header;
    List<String> paths = Arrays.asList("key", "a.a.b", "a.a.c", "a.a.d", "a.b.b", "a.b.c", "a.b.d");
    List<DataType> types =
        Arrays.asList(
            DataType.LONG,
            DataType.INTEGER,
            DataType.LONG,
            DataType.FLOAT,
            DataType.DOUBLE,
            DataType.BOOLEAN,
            DataType.BINARY);
    List<Field> fields = new ArrayList<>();
    for (int i = 1; i < paths.size(); i++) {
      fields.add(new Field(paths.get(i), types.get(i)));
    }
    List<List<Object>> dataRes = new ArrayList<>();
    dataRes.add(new ArrayList<>(paths));
    dataRes.add(types.stream().map(DataType::toString).collect(Collectors.toList()));
    header = new Header(Field.KEY, fields);
    List<Row> rows = new ArrayList<>();
    Object[] objects, objectsWithKey;
    for (int i = 0; i < 10; i++) {
      objects =
          new Object[] {
            i, (long) i + 1, (float) i + 2, (double) i + 3, i % 2 == 0, String.valueOf(i + 1)
          };
      objectsWithKey =
          new Object[] {
            (long) i,
            i,
            (long) i + 1,
            (float) i + 2,
            (double) i + 3,
            i % 2 == 0,
            String.valueOf(i + 1)
          };
      dataRes.add(Arrays.asList(objectsWithKey));
      rows.add(new Row(header, i, objects));
    }
    return new Pair<>(new Table(header, rows), dataRes);
  }

  private Pair<Row, List<List<Object>>> generateRow() {
    Pair<Table, List<List<Object>>> tableListPair = generateTable();
    Table table = tableListPair.k;
    List<List<Object>> expectedRes = tableListPair.v;

    Row row = table.getRow(0);
    List<List<Object>> dataRes = expectedRes.subList(0, 3);
    return new Pair<>(row, dataRes);
  }

  @Test
  public void testConvertTable() {
    Pair<Table, List<List<Object>>> tableListPair = generateTable();
    Table table = tableListPair.k;
    List<List<Object>> expectedRes = tableListPair.v;

    testConvert(table, expectedRes);
    testFail(table);
  }

  @Test
  public void testConvertRow() {
    Pair<Row, List<List<Object>>> rowListPair = generateRow();
    Row row = rowListPair.k;
    List<List<Object>> expectedRes = rowListPair.v;

    testConvert(row, expectedRes);
    testFail(row);
  }

  private void testConvert(Object source, List<List<Object>> expectedRes) {
    // pattern
    List<List<Object>> data = getData(source, Collections.singletonList("a.*"));
    compareResult(expectedRes, data);

    List<List<Object>> expectedRes2 =
        expectedRes.stream().map(e -> e.subList(0, 4)).collect(Collectors.toList());
    data = getData(source, Collections.singletonList("a.a.*"));
    compareResult(expectedRes2, data);

    // path
    data = getData(source, Arrays.asList("a.a.b", "a.a.c", "a.a.d"));
    compareResult(expectedRes2, data);

    // mix
    List<List<Object>> expectedRes3 =
        expectedRes.stream().map(e -> e.subList(0, 6)).collect(Collectors.toList());
    data = getData(source, Arrays.asList("a.a.*", "a.b.b", "a.b.c"));
    compareResult(expectedRes3, data);
  }

  private void testFail(Object source) {
    // pattern
    List<List<Object>> res = getData(source, Arrays.asList("a.c.*", "a.a.b"));
    assertNull(res);

    // path
    res = getData(source, Arrays.asList("a.b.*", "a.c.d"));
    assertNull(res);
  }

  private List<List<Object>> getData(Object source, List<String> pattern) {
    if (source instanceof Table) {
      return DataUtils.dataFromTable((Table) source, pattern);
    } else {
      return DataUtils.dataFromRow((Row) source, pattern);
    }
  }

  private void compareResult(List<List<Object>> expected, List<List<Object>> actual) {
    assertNotNull(expected);
    assertNotNull(actual);
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(new Object[] {expected.get(i)}, new Object[] {actual.get(i)});
    }
  }
}
