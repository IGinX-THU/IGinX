/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.service.storage;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.filestore.common.Configs;
import cn.edu.tsinghua.iginx.filestore.format.raw.RawFormat;
import cn.edu.tsinghua.iginx.filestore.format.raw.RawReaderConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTree;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.test.RowsBuilder;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.io.MoreFiles;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import org.junit.jupiter.api.Test;

public class FileTreeDummyTest extends AbstractDummyTest {

  protected static final String DIR_NAME = "home";

  public FileTreeDummyTest() {
    super(FileTree.NAME, getConfig(), DIR_NAME);
  }

  private static Config getConfig() {
    Map<String, Object> map = new HashMap<>();
    Configs.put(
        map, 8, FileTreeConfig.Fields.formats, RawFormat.NAME, RawReaderConfig.Fields.pageSize);
    Configs.put(map, DIR_NAME, FileTreeConfig.Fields.prefix);
    return ConfigFactory.parseMap(map);
  }

  @Test
  public void testEmptyDir() throws PhysicalException, IOException {
    Files.createDirectories(root);
    reset();

    {
      DataBoundary boundary = getBoundary(null);
      assertEquals(new DataBoundary(), boundary);
    }
    {
      DataBoundary boundary = getBoundary(DIR_NAME);
      assertEquals(new DataBoundary(), boundary);
    }
    {
      DataBoundary boundary = getBoundary("aaa");
      assertEquals(new DataBoundary(), boundary);
    }

    {
      List<String> patterns = null;
      Header schema = getSchema(patterns);
      assertEquals(new Header(Field.KEY, Collections.emptyList()), schema);
      List<Row> rows = query(patterns);
      assertEquals(Collections.emptyList(), rows);
    }
  }

  private static void createFile(Path path, String content) throws IOException {
    MoreFiles.createParentDirectories(path);
    try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE)) {
      out.write(content.getBytes());
    }
  }

  @Test
  public void testSingleFile() throws PhysicalException, IOException {
    createFile(root, "first line: hello world");
    reset();

    {
      DataBoundary boundary = getBoundary(null);
      assertTrue(inBounds(boundary, DIR_NAME));
    }

    {
      DataBoundary boundary = getBoundary(DIR_NAME);
      assertTrue(inBounds(boundary, DIR_NAME));
    }

    {
      DataBoundary boundary = getBoundary("error");
      assertEquals(new DataBoundary(), boundary);
    }

    {
      DataBoundary boundary = getBoundary(null);
      assertTrue(inBounds(boundary, DIR_NAME));
    }

    {
      DataBoundary boundary = getBoundary(DIR_NAME);
      assertTrue(inBounds(boundary, DIR_NAME));
    }

    {
      DataBoundary boundary = getBoundary("error");
      assertEquals(new DataBoundary(), boundary);
    }

    List<Row> expectedRows =
        new RowsBuilder(DIR_NAME).add(0, "first li").add(1, "ne: hell").add(2, "o world").build();

    {
      List<String> patterns = null;
      Header schema = getSchema(patterns);
      Field field = new Field(DIR_NAME, DataType.BINARY);
      assertEquals(new Header(Field.KEY, Collections.singletonList(field)), schema);
      List<Row> rows = query(patterns);
      assertEquals(expectedRows, rows);
    }

    {
      List<String> patterns = Collections.singletonList(DIR_NAME);
      Header schema = getSchema(patterns);
      Field field = new Field(DIR_NAME, DataType.BINARY);
      assertEquals(new Header(Field.KEY, Collections.singletonList(field)), schema);
      List<Row> rows = query(patterns);
      assertEquals(expectedRows, rows);
    }

    {
      List<String> patterns = Collections.singletonList("error");
      Header schema = getSchema(patterns);
      assertEquals(new Header(Field.KEY, Collections.emptyList()), schema);
      List<Row> rows = query(patterns);
      assertEquals(Collections.emptyList(), rows);
    }
  }

  private static Header headerOf(String... fields) {
    List<Field> list = new ArrayList<>();
    for (String field : fields) {
      list.add(new Field(field, DataType.BINARY));
    }
    return new Header(Field.KEY, list);
  }

  @Test
  public void testNestedDirectory() throws PhysicalException, IOException {
    createFile(root.resolve("LICENSE"), "Apache License");
    createFile(root.resolve("README.md"), "this directory is for test");
    createFile(root.resolve("src/main/java/Main.java"), "public class Main {\n}");
    createFile(root.resolve("src/main/java/Tool.java"), "public class Tool {\n}");
    createFile(root.resolve("src/main/resources/config.properties"), "ip=127.0.0.1\nport=6667");
    createFile(root.resolve("src/test/java/Test.java"), "public class Test {\n}");
    Files.createDirectories(root.resolve("src/main/thrift"));
    reset();

    {
      DataBoundary boundary = getBoundary(null);
      assertTrue(inBounds(boundary, "home.LICENSE"));
      assertTrue(inBounds(boundary, "home.README\\md"));
      assertTrue(inBounds(boundary, "home.src.main.java.Main\\java"));
      assertTrue(inBounds(boundary, "home.src.main.java.Tool\\java"));
      assertTrue(inBounds(boundary, "home.src.main.resources.config\\properties"));
      assertTrue(inBounds(boundary, "home.src.test.java.Test\\java"));
    }

    {
      DataBoundary boundary = getBoundary(DIR_NAME);
      assertTrue(inBounds(boundary, "home.LICENSE"));
      assertTrue(inBounds(boundary, "home.README\\md"));
      assertTrue(inBounds(boundary, "home.src.main.java.Main\\java"));
      assertTrue(inBounds(boundary, "home.src.main.java.Tool\\java"));
      assertTrue(inBounds(boundary, "home.src.main.resources.config\\properties"));
      assertTrue(inBounds(boundary, "home.src.test.java.Test\\java"));
    }

    {
      DataBoundary boundary = getBoundary(DIR_NAME + ".src.main.java");
      assertFalse(inBounds(boundary, "home.LICENSE"));
      assertFalse(inBounds(boundary, "home.README\\md"));
      assertTrue(inBounds(boundary, "home.src.main.java.Main\\java"));
      assertTrue(inBounds(boundary, "home.src.main.java.Tool\\java"));
      assertFalse(inBounds(boundary, "home.src.main.resources.config\\properties"));
      assertFalse(inBounds(boundary, "home.src.test.java.Test\\java"));
    }

    {
      DataBoundary boundary = getBoundary("home.src.main.thrift");
      assertEquals(new DataBoundary(), boundary);
    }

    {
      DataBoundary boundary = getBoundary("error");
      assertEquals(new DataBoundary(), boundary);
    }

    // query one column
    {
      Header justMainHeader = getSchema("home.src.main.java.Main\\java");
      List<Row> justMainData =
          new RowsBuilder("home.src.main.java.Main\\java")
              .add(0, "public c")
              .add(1, "lass Mai")
              .add(2, "n {\n}")
              .build();

      {
        List<String> patterns = Collections.singletonList("home.src.main.java.Main\\java");
        Header schema = getSchema(patterns);
        assertEquals(justMainHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(justMainData, rows);
      }

      {
        List<String> patterns = Collections.singletonList("*.Main\\java");
        Header schema = getSchema(patterns);
        assertEquals(justMainHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(justMainData, rows);
      }

      {
        List<String> patterns = Collections.singletonList("*.main.*.Main\\java");
        Header schema = getSchema(patterns);
        assertEquals(justMainHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(justMainData, rows);
      }

      {
        List<String> patterns = Collections.singletonList("home.*.main.*.Main\\java");
        Header schema = getSchema(patterns);
        assertEquals(justMainHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(justMainData, rows);
      }
    }

    // query all column
    {
      Header allHeader =
          headerOf(
              "home.LICENSE",
              "home.README\\md",
              "home.src.main.java.Main\\java",
              "home.src.main.java.Tool\\java",
              "home.src.main.resources.config\\properties",
              "home.src.test.java.Test\\java");

      List<Row> allData =
          new RowsBuilder(
                  "home.LICENSE",
                  "home.README\\md",
                  "home.src.main.java.Main\\java",
                  "home.src.main.java.Tool\\java",
                  "home.src.main.resources.config\\properties",
                  "home.src.test.java.Test\\java")
              .add(0, "Apache L", "this dir", "public c", "public c", "ip=127.0", "public c")
              .add(1, "icense", "ectory i", "lass Mai", "lass Too", ".0.1\npor", "lass Tes")
              .add(2, null, "s for te", "n {\n}", "l {\n}", "t=6667", "t {\n}")
              .add(3, null, "st", null, null, null, null)
              .build();

      {
        List<String> patterns = null;
        Header schema = getSchema(patterns);
        assertEquals(allHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(allData, rows);
      }

      {
        List<String> patterns = Collections.singletonList("*");
        Header schema = getSchema(patterns);
        assertEquals(allHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(allData, rows);
      }

      {
        List<String> patterns = Collections.singletonList("*.*");
        Header schema = getSchema(patterns);
        assertEquals(allHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(allData, rows);
      }

      {
        List<String> patterns = Collections.singletonList("home.*");
        Header schema = getSchema(patterns);
        assertEquals(allHeader, schema);
        List<Row> rows = query(patterns);
        assertEquals(allData, rows);
      }

      {
        List<Row> allDataKey1to2 = Arrays.asList(allData.get(1), allData.get(2));
        List<String> patterns = null;
        Header schema = getSchema(patterns);
        assertEquals(allHeader, schema);
        List<Row> rows =
            query(
                patterns,
                new AndFilter(Arrays.asList(new KeyFilter(Op.GE, 1), new KeyFilter(Op.L, 3))));
        assertEquals(allDataKey1to2, rows);
      }

      {
        List<Row> allDataKey0 = Collections.singletonList(allData.get(0));
        List<String> patterns = null;
        Header schema = getSchema(patterns);
        assertEquals(allHeader, schema);
        {
          List<Row> rows =
              query(
                  patterns,
                  new ValueFilter(
                      "home.src.main.resources.config\\properties", Op.LIKE, new Value(".*[.].*")));
          assertEquals(allDataKey0, rows);
        }
        {
          List<Row> rows = query(patterns, new ValueFilter("*", Op.LIKE, new Value(".*[.].*")));
          assertEquals(allDataKey0, rows);
        }
        {
          List<Row> rows =
              query(patterns, new ValueFilter("home.*", Op.LIKE, new Value(".*[.].*")));
          assertEquals(allDataKey0, rows);
        }
        {
          List<Row> rows =
              query(
                  patterns, new ValueFilter("*.config\\properties", Op.LIKE, new Value(".*[.].*")));
          assertEquals(allDataKey0, rows);
        }
        {
          List<Row> rows =
              query(
                  patterns,
                  new ValueFilter(
                      "home.*.main.*.config\\properties", Op.LIKE, new Value(".*[.].*")));
          assertEquals(allDataKey0, rows);
        }
      }
    }
  }

  @Test
  public void testUpperCase() throws PhysicalException, IOException {
    createFile(root.resolve("async.hpp"), "#include <iostream>");
    createFile(root.resolve("LICENSE"), "Apache License");
    createFile(root.resolve("readme.md"), "this directory is for test");
    reset();
    {
      DataBoundary boundary = getBoundary(null);
      assertTrue(inBounds(boundary, "home.async\\hpp"));
      assertTrue(inBounds(boundary, "home.LICENSE"));
      assertTrue(inBounds(boundary, "home.readme\\md"));
    }
  }
}
