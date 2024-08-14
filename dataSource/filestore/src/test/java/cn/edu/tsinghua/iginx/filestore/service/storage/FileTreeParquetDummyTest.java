package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.filestore.format.parquet.ParquetTestUtils;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTree;
import cn.edu.tsinghua.iginx.filestore.test.TableBuilder;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileTreeParquetDummyTest extends AbstractDummyTest {
  protected final static String DIR_NAME = "home.parquet";

  public FileTreeParquetDummyTest() {
    super(FileTree.NAME, ConfigFactory.empty(), DIR_NAME);
  }

  private void testSingleFile(Path path, String prefix) throws PhysicalException, IOException {
    Table table = new TableBuilder(false, null)
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
      Table expected = new TableBuilder(true, prefix)
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
      Table expected = new TableBuilder(true, prefix)
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
