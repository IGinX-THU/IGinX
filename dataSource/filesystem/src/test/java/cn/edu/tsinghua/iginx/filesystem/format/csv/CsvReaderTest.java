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
package cn.edu.tsinghua.iginx.filesystem.format.csv;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.filesystem.test.DataValidator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CsvReaderTest {

  @Test
  void testTpchLineitemTsv() throws IOException, PhysicalException {
    Path path = Files.createTempFile("testTpchLineitemTsv", ".tsv");
    try (InputStream is =
        getClass().getClassLoader().getResourceAsStream("data/tpch/lineitem.tsv")) {
      if (is == null) {
        throw new IOException("Resource not found");
      }
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
    }

    String prefix = "tpch";
    List<Field> expectedField =
        Arrays.asList(
            new Field(prefix + ".l_orderkey", DataType.LONG),
            new Field(prefix + ".l_partkey", DataType.LONG),
            new Field(prefix + ".l_suppkey", DataType.LONG),
            new Field(prefix + ".l_linenumber", DataType.LONG),
            new Field(prefix + ".l_quantity", DataType.LONG),
            new Field(prefix + ".l_extendedprice", DataType.DOUBLE),
            new Field(prefix + ".l_discount", DataType.DOUBLE),
            new Field(prefix + ".l_tax", DataType.DOUBLE),
            new Field(prefix + ".l_returnflag", DataType.BINARY),
            new Field(prefix + ".l_linestatus", DataType.BINARY),
            new Field(prefix + ".l_shipdate", DataType.LONG),
            new Field(prefix + ".l_commitdate", DataType.LONG),
            new Field(prefix + ".l_receiptdate", DataType.LONG),
            new Field(prefix + ".l_shipinstruct", DataType.BINARY),
            new Field(prefix + ".l_shipmode", DataType.BINARY),
            new Field(prefix + ".l_comment", DataType.BINARY));
    List<String> expectedColumnNames =
        expectedField.stream().map(Field::getName).collect(Collectors.toList());
    Map<String, DataType> expectedColumns =
        expectedField.stream().collect(Collectors.toMap(Field::getName, Field::getType));
    Header expectedHeader = new Header(Field.KEY, expectedField);
    List<Row> expectedRows =
        Arrays.asList(
            new Row(
                expectedHeader,
                0L,
                new Object[] {
                  1L,
                  156L,
                  4L,
                  1L,
                  17L,
                  17954.55,
                  0.04,
                  0.02,
                  "N",
                  "O",
                  826646400000L,
                  824054400000L,
                  827424000000L,
                  "DELIVER IN PERSON",
                  "TRUCK",
                  "egular courts above the"
                }),
            new Row(
                expectedHeader,
                1L,
                new Object[] {
                  1L,
                  68L,
                  9L,
                  2L,
                  36L,
                  34850.16,
                  0.09,
                  0.06,
                  "N",
                  "O",
                  829238400000L,
                  825436800000L,
                  829929600000L,
                  "TAKE BACK RETURN",
                  "MAIL",
                  "ly final dependencies: slyly bold "
                }),
            new Row(
                expectedHeader,
                2L,
                new Object[] {
                  1L,
                  64L,
                  5L,
                  3L,
                  8L,
                  7712.48,
                  0.10,
                  0.02,
                  "N",
                  "O",
                  822844800000L,
                  825955200000L,
                  823017600000L,
                  "TAKE BACK RETURN",
                  "REG AIR",
                  "riously. regular, express dep"
                }));

    CsvReaderConfig config = new CsvReaderConfig();
    config.setInferSchema(true);
    config.setParseTypeFromHeader(false);

    try (CsvReader reader = new CsvReader(prefix, path, config)) {
      Map<String, DataType> map = reader.find(Collections.singletonList("*"));
      assertEquals(expectedColumns, map);
      List<Row> rows = new ArrayList<>();
      try (RowStream rowStream = reader.read(expectedColumnNames, new KeyFilter(Op.L, 3))) {
        assertEquals(expectedHeader, rowStream.getHeader());
        while (rowStream.hasNext()) {
          rows.add(rowStream.next());
        }
      }
      assertEquals(expectedRows, DataValidator.withBinaryAsString(rows));
    }
  }

  @Test
  void testTpchLineitemTsvWithTypeHint() throws IOException, PhysicalException {
    Path path = Files.createTempFile("testTpchLineitemTsvWithTypeHint", ".csv");
    try (InputStream is =
        getClass().getClassLoader().getResourceAsStream("data/tpch/lineitem_hint.csv")) {
      if (is == null) {
        throw new IOException("Resource not found");
      }
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
    }

    String prefix = "tpch";
    List<Field> expectedField =
        Arrays.asList(
            new Field(prefix + ".l_orderkey", DataType.INTEGER),
            new Field(prefix + ".l_partkey", DataType.INTEGER),
            new Field(prefix + ".l_suppkey", DataType.INTEGER),
            new Field(prefix + ".l_linenumber", DataType.INTEGER),
            new Field(prefix + ".l_quantity", DataType.DOUBLE),
            new Field(prefix + ".l_extendedprice", DataType.DOUBLE),
            new Field(prefix + ".l_discount", DataType.DOUBLE),
            new Field(prefix + ".l_tax", DataType.DOUBLE),
            new Field(prefix + ".l_returnflag", DataType.BINARY),
            new Field(prefix + ".l_linestatus", DataType.BINARY),
            new Field(prefix + ".l_shipdate", DataType.LONG),
            new Field(prefix + ".l_commitdate", DataType.LONG),
            new Field(prefix + ".l_receiptdate", DataType.LONG),
            new Field(prefix + ".l_shipinstruct", DataType.BINARY),
            new Field(prefix + ".l_shipmode", DataType.BINARY),
            new Field(prefix + ".l_comment", DataType.BINARY));
    List<String> expectedColumnNames =
        expectedField.stream().map(Field::getName).collect(Collectors.toList());
    Map<String, DataType> expectedColumns =
        expectedField.stream().collect(Collectors.toMap(Field::getName, Field::getType));
    Header expectedHeader = new Header(Field.KEY, expectedField);
    List<Row> expectedRows =
        Arrays.asList(
            new Row(
                expectedHeader,
                0L,
                new Object[] {
                  1,
                  156,
                  4,
                  1,
                  17.0,
                  17954.55,
                  0.04,
                  0.02,
                  "N",
                  "O",
                  826646400000L,
                  824054400000L,
                  827424000000L,
                  "DELIVER IN PERSON",
                  "TRUCK",
                  "egular courts above the"
                }),
            new Row(
                expectedHeader,
                1L,
                new Object[] {
                  1,
                  68,
                  9,
                  2,
                  36.0,
                  34850.16,
                  0.09,
                  0.06,
                  "N",
                  "O",
                  829238400000L,
                  825436800000L,
                  829929600000L,
                  "TAKE BACK RETURN",
                  "MAIL",
                  "ly final dependencies: slyly bold "
                }),
            new Row(
                expectedHeader,
                2L,
                new Object[] {
                  1,
                  64,
                  5,
                  3,
                  8.0,
                  7712.48,
                  0.10,
                  0.02,
                  "N",
                  "O",
                  822844800000L,
                  825955200000L,
                  823017600000L,
                  "TAKE BACK RETURN",
                  "REG AIR",
                  "riously. regular, express dep"
                }));

    try (CsvReader reader = new CsvReader(prefix, path, new CsvReaderConfig())) {
      Map<String, DataType> map = reader.find(Collections.singletonList("*"));
      assertEquals(expectedColumns, map);
      List<Row> rows = new ArrayList<>();
      try (RowStream rowStream = reader.read(expectedColumnNames, new KeyFilter(Op.L, 3))) {
        assertEquals(expectedHeader, rowStream.getHeader());
        while (rowStream.hasNext()) {
          rows.add(rowStream.next());
        }
      }
      assertEquals(expectedRows, DataValidator.withBinaryAsString(rows));
    }
  }

  @Test
  void testCustomDatetimeFormat() throws IOException, PhysicalException {
    Path path = Files.createTempFile("testTpchLineitemTsvWithCustomCustomDatetime", ".csv");
    try (InputStream is =
        getClass().getClassLoader().getResourceAsStream("data/tpch/lineitem_customdatetime.csv")) {
      if (is == null) {
        throw new IOException("Resource not found");
      }
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
    }

    String prefix = "tpch";
    List<Field> expectedField =
        Arrays.asList(
            new Field(prefix + ".l_orderkey", DataType.INTEGER),
            new Field(prefix + ".l_partkey", DataType.INTEGER),
            new Field(prefix + ".l_suppkey", DataType.INTEGER),
            new Field(prefix + ".l_linenumber", DataType.INTEGER),
            new Field(prefix + ".l_quantity", DataType.DOUBLE),
            new Field(prefix + ".l_extendedprice", DataType.DOUBLE),
            new Field(prefix + ".l_discount", DataType.DOUBLE),
            new Field(prefix + ".l_tax", DataType.DOUBLE),
            new Field(prefix + ".l_returnflag", DataType.BINARY),
            new Field(prefix + ".l_linestatus", DataType.BINARY),
            new Field(prefix + ".l_shipdate", DataType.LONG),
            new Field(prefix + ".l_commitdate", DataType.LONG),
            new Field(prefix + ".l_receiptdate", DataType.LONG),
            new Field(prefix + ".l_shipinstruct", DataType.BINARY),
            new Field(prefix + ".l_shipmode", DataType.BINARY),
            new Field(prefix + ".l_comment", DataType.BINARY));
    List<String> expectedColumnNames =
        expectedField.stream().map(Field::getName).collect(Collectors.toList());
    Map<String, DataType> expectedColumns =
        expectedField.stream().collect(Collectors.toMap(Field::getName, Field::getType));
    Header expectedHeader = new Header(Field.KEY, expectedField);
    List<Row> expectedRows =
        Arrays.asList(
            new Row(
                expectedHeader,
                0L,
                new Object[] {
                  1,
                  156,
                  4,
                  1,
                  17.0,
                  17954.55,
                  0.04,
                  0.02,
                  "N",
                  "O",
                  826646400000L,
                  824054400000L,
                  827424000000L,
                  "DELIVER IN PERSON",
                  "TRUCK",
                  "egular courts above the"
                }),
            new Row(
                expectedHeader,
                1L,
                new Object[] {
                  1,
                  68,
                  9,
                  2,
                  36.0,
                  34850.16,
                  0.09,
                  0.06,
                  "N",
                  "O",
                  829238400000L,
                  825436800000L,
                  829929600000L,
                  "TAKE BACK RETURN",
                  "MAIL",
                  "ly final dependencies: slyly bold "
                }),
            new Row(
                expectedHeader,
                2L,
                new Object[] {
                  1,
                  64,
                  5,
                  3,
                  8.0,
                  7712.48,
                  0.10,
                  0.02,
                  "N",
                  "O",
                  822844800000L,
                  825955200000L,
                  823017600000L,
                  "TAKE BACK RETURN",
                  "REG AIR",
                  "riously. regular, express dep"
                }));
    CsvReaderConfig config = new CsvReaderConfig();
    config.setDateFormat("yyyy/MM/dd");

    try (CsvReader reader = new CsvReader(prefix, path, config)) {
      Map<String, DataType> map = reader.find(Collections.singletonList("*"));
      assertEquals(expectedColumns, map);
      List<Row> rows = new ArrayList<>();
      try (RowStream rowStream = reader.read(expectedColumnNames, new KeyFilter(Op.L, 3))) {
        assertEquals(expectedHeader, rowStream.getHeader());
        while (rowStream.hasNext()) {
          rows.add(rowStream.next());
        }
      }
      assertEquals(expectedRows, DataValidator.withBinaryAsString(rows));
    }
  }

  @Test
  void testAllowDuplicateColumnNamesFalse() throws IOException, PhysicalException {
    Path path = Files.createTempFile("testTpchLineitemTsvWithDuplicateColumns", ".csv");
    try (InputStream is =
        getClass()
            .getClassLoader()
            .getResourceAsStream("data/tpch/lineitem_duplicatecolumns.csv")) {
      if (is == null) {
        throw new IOException("Resource not found");
      }
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
    }
    CsvReaderConfig config = new CsvReaderConfig();
    config.setAllowDuplicateColumnNames(false);

    assertThrows(IOException.class, () -> new CsvReader("", path, config));
  }

  @Test
  void testAllowDuplicateColumnNamesTrue() throws IOException, PhysicalException {
    Path path = Files.createTempFile("testTpchLineitemTsvWithDuplicateColumns", ".csv");
    try (InputStream is =
        getClass()
            .getClassLoader()
            .getResourceAsStream("data/tpch/lineitem_duplicatecolumns.csv")) {
      if (is == null) {
        throw new IOException("Resource not found");
      }
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
    }

    String prefix = "tpch";
    List<Field> expectedField =
        Arrays.asList(
            new Field(prefix + ".l_orderkey_1", DataType.INTEGER),
            new Field(prefix + ".l_orderkey_2", DataType.INTEGER),
            new Field(prefix + ".l_orderkey_3", DataType.INTEGER),
            new Field(prefix + ".l_partkey", DataType.INTEGER),
            new Field(prefix + ".l_suppkey", DataType.INTEGER),
            new Field(prefix + ".l_linenumber", DataType.INTEGER),
            new Field(prefix + ".l_quantity", DataType.DOUBLE),
            new Field(prefix + ".l_extendedprice", DataType.DOUBLE),
            new Field(prefix + ".l_discount", DataType.DOUBLE),
            new Field(prefix + ".l_tax", DataType.DOUBLE),
            new Field(prefix + ".l_returnflag", DataType.BINARY),
            new Field(prefix + ".l_linestatus", DataType.BINARY),
            new Field(prefix + ".l_shipdate", DataType.LONG),
            new Field(prefix + ".l_commitdate", DataType.LONG),
            new Field(prefix + ".l_receiptdate", DataType.LONG),
            new Field(prefix + ".l_shipinstruct", DataType.BINARY),
            new Field(prefix + ".l_shipmode", DataType.BINARY),
            new Field(prefix + ".l_comment", DataType.BINARY));
    List<String> expectedColumnNames =
        expectedField.stream().map(Field::getName).collect(Collectors.toList());
    Map<String, DataType> expectedColumns =
        expectedField.stream().collect(Collectors.toMap(Field::getName, Field::getType));
    Header expectedHeader = new Header(Field.KEY, expectedField);
    List<Row> expectedRows =
        Arrays.asList(
            new Row(
                expectedHeader,
                0L,
                new Object[] {
                  1,
                  2,
                  3,
                  156,
                  4,
                  1,
                  17.0,
                  17954.55,
                  0.04,
                  0.02,
                  "N",
                  "O",
                  826646400000L,
                  824054400000L,
                  827424000000L,
                  "DELIVER IN PERSON",
                  "TRUCK",
                  "egular courts above the"
                }),
            new Row(
                expectedHeader,
                1L,
                new Object[] {
                  1,
                  2,
                  3,
                  68,
                  9,
                  2,
                  36.0,
                  34850.16,
                  0.09,
                  0.06,
                  "N",
                  "O",
                  829238400000L,
                  825436800000L,
                  829929600000L,
                  "TAKE BACK RETURN",
                  "MAIL",
                  "ly final dependencies: slyly bold "
                }),
            new Row(
                expectedHeader,
                2L,
                new Object[] {
                  1,
                  2,
                  3,
                  64,
                  5,
                  3,
                  8.0,
                  7712.48,
                  0.10,
                  0.02,
                  "N",
                  "O",
                  822844800000L,
                  825955200000L,
                  823017600000L,
                  "TAKE BACK RETURN",
                  "REG AIR",
                  "riously. regular, express dep"
                }));
    CsvReaderConfig config = new CsvReaderConfig();
    config.setAllowDuplicateColumnNames(true);

    try (CsvReader reader = new CsvReader(prefix, path, config)) {
      Map<String, DataType> map = reader.find(Collections.singletonList("*"));
      assertEquals(expectedColumns, map);
      List<Row> rows = new ArrayList<>();
      try (RowStream rowStream = reader.read(expectedColumnNames, new KeyFilter(Op.L, 3))) {
        assertEquals(expectedHeader, rowStream.getHeader());
        while (rowStream.hasNext()) {
          rows.add(rowStream.next());
        }
      }
      assertEquals(expectedRows, DataValidator.withBinaryAsString(rows));
    }
  }
}
