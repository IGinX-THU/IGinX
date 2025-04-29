package cn.edu.tsinghua.iginx.filesystem.format.csv;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.filesystem.test.DataValidator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CsvReaderTest {

  @Test
  void testTpchLineitemTsv() throws IOException, PhysicalException {
    Path path = Files.createTempFile("testTpchLineitemTsv",".tsv");
    try(InputStream is = getClass().getClassLoader().getResourceAsStream("data/tpch/lineitem.tsv")) {
      if(is == null) {
        throw new IOException("Resource not found");
      }
      Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
    }

    String prefix = "tpch";
    List<Field> expectedField = Arrays.asList(
        new Field(prefix+".l_orderkey", DataType.LONG),
        new Field(prefix+".l_partkey", DataType.LONG),
        new Field(prefix+".l_suppkey", DataType.LONG),
        new Field(prefix+".l_linenumber", DataType.LONG),
        new Field(prefix+".l_quantity", DataType.LONG),
        new Field(prefix+".l_extendedprice", DataType.DOUBLE),
        new Field(prefix+".l_discount", DataType.DOUBLE),
        new Field(prefix+".l_tax", DataType.DOUBLE),
        new Field(prefix+".l_returnflag", DataType.BINARY),
        new Field(prefix+".l_linestatus", DataType.BINARY),
        new Field(prefix+".l_shipdate", DataType.LONG),
        new Field(prefix+".l_commitdate", DataType.LONG),
        new Field(prefix+".l_receiptdate", DataType.LONG),
        new Field(prefix+".l_shipinstruct", DataType.BINARY),
        new Field(prefix+".l_shipmode", DataType.BINARY),
        new Field(prefix+".l_comment", DataType.BINARY)
    );
    List<String> expectedColumnNames = expectedField.stream().map(Field::getName).collect(Collectors.toList());
    Map<String, DataType> expectedColumns = expectedField.stream().collect(Collectors.toMap(Field::getName, Field::getType));
    Header expectedHeader = new Header(Field.KEY, expectedField);
    List<Row> expectedRows = Arrays.asList(
        new Row(expectedHeader,0L,new Object[]{1L,156L,4L,1L,17L,17954.55,0.04,0.02,"N","O",826646400000L,824054400000L,827424000000L,"DELIVER IN PERSON","TRUCK","egular courts above the"}),
        new Row(expectedHeader,1L,new Object[]{1L,68L,9L,2L,36L,34850.16,0.09,0.06,"N","O",829238400000L, 825436800000L, 829929600000L,"TAKE BACK RETURN","MAIL","ly final dependencies: slyly bold "}),
        new Row(expectedHeader,2L,new Object[]{1L,64L,5L,3L,8L,7712.48,0.10,0.02,"N","O",822844800000L,825955200000L,823017600000L,"TAKE BACK RETURN","REG AIR","riously. regular, express dep"})
    );

    try(CsvReader reader = new CsvReader(prefix,path,new CsvReaderConfig())){
      Map<String, DataType> map = reader.find(Collections.singletonList("*"));
      assertEquals(expectedColumns, map);
      List<Row> rows = new ArrayList<>();
      try(RowStream rowStream = reader.read(expectedColumnNames,new KeyFilter(Op.L,3))){
        assertEquals(expectedHeader, rowStream.getHeader());
        while (rowStream.hasNext()) {
          rows.add(rowStream.next());
        }
      }
      assertEquals(expectedRows, DataValidator.withBinaryAsString(rows));
    }
  }

}