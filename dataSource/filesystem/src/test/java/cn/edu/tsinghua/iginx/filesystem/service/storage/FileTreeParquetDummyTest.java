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
package cn.edu.tsinghua.iginx.filesystem.service.storage;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Configs;
import cn.edu.tsinghua.iginx.filesystem.format.parquet.ParquetTestUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.FileTree;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filesystem.test.TableBuilder;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;

public class FileTreeParquetDummyTest extends AbstractDummyTest {
  protected static final String DIR_NAME = "home.parquet";

  public FileTreeParquetDummyTest() {
    super(FileTree.NAME, getConfig(), DIR_NAME);
  }

  private static Config getConfig() {
    Map<String, Object> map = new HashMap<>();
    Configs.put(map, "home\\parquet", FileTreeConfig.Fields.prefix);
    return ConfigFactory.parseMap(map);
  }

  private void testSingleFile(Path path, String prefix) throws PhysicalException, IOException {
    Table table =
        new TableBuilder(false, null)
            .names("deleted", "dist", "id", "income", "name", "year")
            .types(BOOLEAN, FLOAT, LONG, DOUBLE, BINARY, INTEGER)
            .row(false, 1.1f, 9001L, 2000.02, "Alice", 1993)
            .row(true, 7.2f, 9002L, 1000.30, "Bob", 1991)
            .row(false, 2.8f, 9003L, 5000.4, "Charlie", 1980)
            .row(true, 1.4f, 9004L, 4000.5, "David", 1992)
            .row(false, 6.5f, 9005L, 3000.6, "Eve", 2001)
            .build();

    ParquetTestUtils.createFile(path, table);
    reset();

    {
      DataBoundary boundary = getBoundary(null);
      assertTrue(inBounds(boundary, prefix + ".id"));
      assertTrue(inBounds(boundary, prefix + ".name"));
      assertTrue(inBounds(boundary, prefix + ".year"));
      assertTrue(inBounds(boundary, prefix + ".dist"));
      assertTrue(inBounds(boundary, prefix + ".income"));
      assertTrue(inBounds(boundary, prefix + ".deleted"));
    }

    {
      Table expected =
          new TableBuilder(true, prefix)
              .names("deleted", "dist", "id", "income", "name", "year")
              .types(BOOLEAN, FLOAT, LONG, DOUBLE, BINARY, INTEGER)
              .key(0L, false, 1.1f, 9001L, 2000.02, "Alice", 1993)
              .key(1L, true, 7.2f, 9002L, 1000.30, "Bob", 1991)
              .key(2L, false, 2.8f, 9003L, 5000.4, "Charlie", 1980)
              .key(3L, true, 1.4f, 9004L, 4000.5, "David", 1992)
              .key(4L, false, 6.5f, 9005L, 3000.6, "Eve", 2001)
              .build();

      {
        List<String> patterns = null;
        Header schema = getSchema(patterns);
        assertEquals(expected.getHeader(), schema);
        List<Row> rows = query(patterns);
        assertEquals(expected.getRows(), rows);
      }

      List<Row> firstRowOfExpected = Collections.singletonList(expected.getRows().get(0));

      {
        List<String> patterns = null;
        Header schema = getSchema(patterns);
        assertEquals(expected.getHeader(), schema);
        List<Row> rows = query(patterns, new ValueFilter(prefix + ".id", Op.E, new Value(9001L)));
        assertEquals(firstRowOfExpected, rows);
      }

      {
        List<String> patterns = null;
        Header schema = getSchema(patterns);
        assertEquals(expected.getHeader(), schema);
        List<Row> rows = query(patterns, new KeyFilter(Op.L, 1L));
        assertEquals(firstRowOfExpected, rows);
      }
    }

    {
      Table expected =
          new TableBuilder(true, prefix)
              .names("id", "name")
              .types(LONG, BINARY)
              .key(0L, 9001L, "Alice")
              .key(1L, 9002L, "Bob")
              .key(2L, 9003L, "Charlie")
              .key(3L, 9004L, "David")
              .key(4L, 9005L, "Eve")
              .build();

      {
        List<String> patterns = Arrays.asList(prefix + ".id", prefix + ".name");
        Header schema = getSchema(patterns);
        assertEquals(expected.getHeader(), schema);
        List<Row> rows = query(patterns);
        assertEquals(expected.getRows(), rows);
      }
    }

    {
      List<String> patterns = Collections.singletonList(prefix + ".error");
      Header schema = getSchema(patterns);
      assertEquals(new Header(Field.KEY, Collections.emptyList()), schema);
      List<Row> rows = query(patterns);
      assertEquals(Collections.emptyList(), rows);
    }
  }

  @Test
  public void testSingleFileAsRoot() throws PhysicalException, IOException {
    testSingleFile(root, "home\\parquet");
  }

  @Test
  public void testNestedSingleFile() throws PhysicalException, IOException {
    testSingleFile(root.resolve("user.parquet"), "home\\parquet.user\\parquet");
  }
}
