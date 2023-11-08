package cn.edu.tsinghua.iginx.parquet.io;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.parquet.entity.InMemoryTable;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetIOTest {

  private static final Logger logger = LoggerFactory.getLogger(ParquetIOTest.class);

  protected static Path FILE_PATH = Paths.get("./src/test/resources/test.parquet");

  @Before
  public void setUp() throws Exception {
    if (new File(FILE_PATH.toString()).createNewFile()) {
      logger.info("Create file: " + FILE_PATH.toString());
    }
  }

  //  @After
  //  public void tearDown() throws Exception {
  //    if (Files.deleteIfExists(FILE_PATH)) {
  //      logger.info("Delete file: " + FILE_PATH.toString());
  //    }
  //  }

  public static List<Map.Entry<String, DataType>> createHeader(String[] names, DataType[] types) {
    assert names.length == types.length;
    List<Map.Entry<String, DataType>> result = new ArrayList<>();
    for (int i = 0; i < names.length; i++) {
      result.add(new AbstractMap.SimpleImmutableEntry<>(names[i], types[i]));
    }
    return result;
  }

  public static InMemoryTable createInMemoryTable(
      List<Map.Entry<String, DataType>> header, int num, long seed, double filled) {
    if (seed <= 0) {
      seed = -seed + 1;
    }
    InMemoryTable table = new InMemoryTable(header);
    Random rand = new Random(seed);
    long key = seed;
    for (int i = 0; i < num; i++) {
      for (int j = 0; j < header.size(); j++) {
        if (rand.nextDouble() > filled) {
          continue;
        }
        Object value = null;
        switch (header.get(j).getValue()) {
          case BOOLEAN:
            value = rand.nextBoolean();
            break;
          case INTEGER:
            value = rand.nextInt();
            break;
          case LONG:
            value = rand.nextLong();
            break;
          case FLOAT:
            value = rand.nextFloat();
            break;
          case DOUBLE:
            value = rand.nextDouble();
            break;
          case BINARY:
            byte[] bytes = new byte[rand.nextInt(256)];
            rand.nextBytes(bytes);
            value = bytes;
            break;
          default:
            throw new RuntimeException("unsupported data type: " + header.get(j).getValue());
        }
        table.put(j, key, value);
      }
      key += seed;
    }
    return table;
  }

  public static void flushInMemoryTable(InMemoryTable memTable, Path filePath) throws Exception {
    MessageType schema = SchemaUtils.getMessageTypeStartWithKey("test", memTable.getHeader());
    IginxParquetWriter.Builder writerBuilder = IginxParquetWriter.builder(filePath, schema);

    try (IginxParquetWriter writer = writerBuilder.build()) {
      long lastKey = Long.MIN_VALUE;
      IginxRecord record = null;
      for (InMemoryTable.Point point : memTable.scanRows()) {
        if (record != null && point.key != lastKey) {
          writer.write(record);
          record = null;
        }
        if (record == null) {
          record = new IginxRecord();
          record.add(0, point.key);
          lastKey = point.key;
        }
        record.add(point.field + 1, point.value);
      }
      if (record != null) {
        writer.write(record);
      }
    }
  }

  public static InMemoryTable loadInMemoryTable(Path filePath) throws Exception {

    IginxParquetReader.Builder builder = IginxParquetReader.builder(filePath);
    try (IginxParquetReader reader = builder.build()) {
      MessageType schema = reader.getSchema();

      Integer keyIndex = SchemaUtils.getFieldIndex(schema, SchemaUtils.KEY_FIELD_NAME);
      assert keyIndex != null;

      Integer[] projected = new Integer[schema.getFieldCount()];
      List<Map.Entry<String, DataType>> header = new ArrayList<>();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        if (keyIndex == i) {
          continue;
        }
        projected[i] = header.size();
        PrimitiveType parquetType = schema.getType(i).asPrimitiveType();
        header.add(
            new AbstractMap.SimpleImmutableEntry<>(
                schema.getFieldName(i),
                SchemaUtils.getIginxType(parquetType.getPrimitiveTypeName())));
      }

      InMemoryTable table = new InMemoryTable(header);

      IginxRecord record;
      while ((record = reader.read()) != null) {
        Long key = null;
        for (Map.Entry<Integer, Object> entry : record) {
          Integer index = entry.getKey();
          Object value = entry.getValue();
          if (index.equals(keyIndex)) {
            key = (Long) value;
            break;
          }
        }
        assert key != null;

        for (Map.Entry<Integer, Object> entry : record) {
          Integer index = entry.getKey();
          if (index.equals(keyIndex)) {
            continue;
          }
          Object value = entry.getValue();
          Integer fieldIndex = projected[index];
          assert fieldIndex != null;
          table.put(fieldIndex, key, value);
        }
      }
      return table;
    }
  }

  @Test
  public void testWriteReadSimpleInMemoryTable() throws Exception {
    List<Map.Entry<String, DataType>> header =
        createHeader(
            new String[] {"s1", "s2", "s3"},
            new DataType[] {DataType.LONG, DataType.DOUBLE, DataType.BINARY});
    InMemoryTable memTable = createInMemoryTable(header, 10, 7, 1);
    flushInMemoryTable(memTable, FILE_PATH);
    InMemoryTable fileTable = loadInMemoryTable(FILE_PATH);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadCompleteInMemoryTable() throws Exception {
    List<Map.Entry<String, DataType>> header =
        createHeader(
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new DataType[] {
              DataType.BOOLEAN,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.BINARY
            });
    InMemoryTable memTable = createInMemoryTable(header, 10, 7, 1);
    flushInMemoryTable(memTable, FILE_PATH);
    InMemoryTable fileTable = loadInMemoryTable(FILE_PATH);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadSparseInMemoryTable() throws Exception {
    List<Map.Entry<String, DataType>> header =
        createHeader(
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new DataType[] {
              DataType.BOOLEAN,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.BINARY
            });
    InMemoryTable memTable = createInMemoryTable(header, 10, 7, 0.2);
    flushInMemoryTable(memTable, FILE_PATH);
    InMemoryTable fileTable = loadInMemoryTable(FILE_PATH);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadLargeInMemoryTable() throws Exception {
    List<Map.Entry<String, DataType>> header =
        createHeader(
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new DataType[] {
              DataType.BOOLEAN,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.BINARY
            });
    InMemoryTable memTable = createInMemoryTable(header, 100000, 7, 0.5);
    flushInMemoryTable(memTable, FILE_PATH);
    InMemoryTable fileTable = loadInMemoryTable(FILE_PATH);
    assertEquals(memTable, fileTable);
  }
}
