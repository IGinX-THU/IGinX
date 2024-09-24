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

import static cn.edu.tsinghua.iginx.filestore.common.DataUnits.of;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileStructureManager;
import cn.edu.tsinghua.iginx.filestore.test.DataViewGenerator;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHistoryDataTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHistoryDataTest.class);

  @BeforeAll
  public static void beforeAll() {
    LOGGER.info("loaded structures: {}", FileStructureManager.getInstance().getAll());
  }

  private final Path root;
  private final StorageConfig historyConfig;
  private final StorageConfig dummyConfig;

  protected AbstractHistoryDataTest(String type, Config history, Config dummyConfig) {
    this.root = Paths.get("target", "test", UUID.randomUUID().toString());
    this.historyConfig = new StorageConfig(root.toString(), type, history);
    this.dummyConfig = new StorageConfig(root.toString(), type, dummyConfig);
  }

  @BeforeEach
  public void beforeEach() throws IOException {
    Files.createDirectories(root);
  }

  @AfterEach
  public void afterEach() throws IOException {
    MoreFiles.deleteRecursively(root, RecursiveDeleteOption.ALLOW_INSECURE);
  }

  protected StorageService newHistoryService() throws FileStoreException {
    return new StorageService(historyConfig, null);
  }

  protected StorageService newDummyService() throws FileStoreException {
    return new StorageService(null, dummyConfig);
  }

  @Test
  public void testNoData() throws FileStoreException {
    try (StorageService service = newDummyService()) {
      Map<DataUnit, DataBoundary> units = service.getUnits(null);
      Map<DataUnit, DataBoundary> expected =
          Collections.singletonMap(of(true, null), new DataBoundary());
      Assertions.assertEquals(expected, units);
    }
  }

  @Test
  public void testSingleUnit() throws PhysicalException {
    DataUnit unit = of(false, "unit0000");
    DataView data =
        DataViewGenerator.genRowDataViewNoKey(
            0,
            Arrays.asList("name", "phone"),
            Arrays.asList(Collections.singletonMap("language", "en"), Collections.emptyMap()),
            Arrays.asList(DataType.BINARY, DataType.LONG),
            Arrays.asList(
                new Object[] {"Bob".getBytes(), 1234567L},
                new Object[] {"Alice".getBytes(), 2345678L}));
    try (StorageService service = newHistoryService()) {
      service.insert(unit, data);
    }
    try (StorageService service = newDummyService()) {
      // test getUnits
      Map<DataUnit, DataBoundary> units = service.getUnits(null);
      KeyInterval interval = KeyInterval.getDefaultKeyInterval();
      DataBoundary boundary = new DataBoundary(interval.getStartKey(), interval.getEndKey());
      boundary.setStartColumn("name");
      boundary.setEndColumn("phone~");
      Map<DataUnit, DataBoundary> expected = Collections.singletonMap(of(true, null), boundary);
      Assertions.assertEquals(expected, units);

      // test show columns
      try (RowStream result =
          service.query(
              of(true, null),
              new DataTarget(new BoolFilter(false), Collections.singletonList("*"), null),
              null)) {
        Assertions.assertFalse(result.hasNext());
        Assertions.assertTrue(result.getHeader().hasKey());
        Assertions.assertEquals(
            new HashSet<>(
                Arrays.asList(
                    new Field("name", DataType.BINARY, Collections.singletonMap("language", "en")),
                    new Field("phone", DataType.LONG))),
            new HashSet<>(result.getHeader().getFields()));
      }

      // test query data with tag
      try (RowStream result =
          service.query(
              of(true, null),
              new DataTarget(
                  null, Collections.singletonList("*"), new BaseTagFilter("language", "en")),
              null)) {
        Assertions.assertTrue(result.hasNext());
        Assertions.assertEquals(
            Collections.singletonList(
                new Field("name", DataType.BINARY, Collections.singletonMap("language", "en"))),
            result.getHeader().getFields());
        Assertions.assertArrayEquals(new Object[] {"Bob".getBytes()}, result.next().getValues());
        Assertions.assertArrayEquals(new Object[] {"Alice".getBytes()}, result.next().getValues());
        Assertions.assertFalse(result.hasNext());
      }
    }
  }

  @Test
  public void testMultipleUnits() throws PhysicalException {
    Map<DataUnit, DataView> dataMap = new HashMap<>();
    {
      DataUnit unit = of(false, "unit0000");
      DataView data =
          DataViewGenerator.genRowDataViewNoKey(
              0,
              Arrays.asList("address", "year"),
              null,
              Arrays.asList(DataType.BINARY, DataType.INTEGER),
              Arrays.asList(
                  new Object[] {"Beijing".getBytes(), 2021},
                  new Object[] {"Shanghai".getBytes(), 2022}));
      dataMap.put(unit, data);
    }
    {
      DataUnit unit = of(false, "unit0001");
      DataView data =
          DataViewGenerator.genRowDataViewNoKey(
              0,
              Arrays.asList("name", "phone"),
              Arrays.asList(Collections.singletonMap("language", "en"), Collections.emptyMap()),
              Arrays.asList(DataType.BINARY, DataType.LONG),
              Arrays.asList(
                  new Object[] {"Charlie".getBytes(), 3456789L},
                  new Object[] {"David".getBytes(), 4567890L}));
      dataMap.put(unit, data);
    }
    {
      DataUnit unit = of(false, "unit0002");
      DataView data =
          DataViewGenerator.genRowDataViewNoKey(
              1,
              Arrays.asList("name", "phone"),
              Arrays.asList(Collections.singletonMap("language", "en"), Collections.emptyMap()),
              Arrays.asList(DataType.BINARY, DataType.LONG),
              Arrays.asList(
                  new Object[] {"Eve".getBytes(), 5678901L},
                  new Object[] {"Frank".getBytes(), 6789012L}));
      dataMap.put(unit, data);
    }
    try (StorageService service = newHistoryService()) {
      for (Map.Entry<DataUnit, DataView> entry : dataMap.entrySet()) {
        service.insert(entry.getKey(), entry.getValue());
      }
    }
    try (StorageService service = newDummyService()) {
      // test getUnits
      Map<DataUnit, DataBoundary> units = service.getUnits(null);
      KeyInterval interval = KeyInterval.getDefaultKeyInterval();
      DataBoundary boundary = new DataBoundary(interval.getStartKey(), interval.getEndKey());
      boundary.setStartColumn("address");
      boundary.setEndColumn("year~");
      Map<DataUnit, DataBoundary> expected = Collections.singletonMap(of(true, null), boundary);
      Assertions.assertEquals(expected, units);

      // test show columns
      try (RowStream result =
          service.query(
              of(true, null),
              new DataTarget(new BoolFilter(false), Collections.singletonList("*"), null),
              null)) {
        Assertions.assertFalse(result.hasNext());
        Assertions.assertTrue(result.getHeader().hasKey());
        Assertions.assertEquals(
            new HashSet<>(
                Arrays.asList(
                    new Field("name", DataType.BINARY, Collections.singletonMap("language", "en")),
                    new Field("phone", DataType.LONG),
                    new Field("address", DataType.BINARY),
                    new Field("year", DataType.INTEGER))),
            new HashSet<>(result.getHeader().getFields()));
      }

      // query all data
      try (RowStream result =
          service.query(
              of(true, null), new DataTarget(null, Collections.singletonList("*"), null), null)) {
        Assertions.assertEquals(
            new HashSet<>(
                Arrays.asList(
                    new Field("name", DataType.BINARY, Collections.singletonMap("language", "en")),
                    new Field("phone", DataType.LONG),
                    new Field("address", DataType.BINARY),
                    new Field("year", DataType.INTEGER))),
            new HashSet<>(result.getHeader().getFields()));
        Assertions.assertTrue(result.hasNext());
        result.next();
        Assertions.assertTrue(result.hasNext());
        result.next();
        Assertions.assertTrue(result.hasNext());
        result.next();
        Assertions.assertFalse(result.hasNext());
      }

      // query overlap
      try (RowStream result =
          service.query(
              of(true, null),
              new DataTarget(null, Collections.singletonList("name"), null),
              null)) {
        Assertions.assertEquals(
            Collections.singletonList(
                new Field("name", DataType.BINARY, Collections.singletonMap("language", "en"))),
            result.getHeader().getFields());
        Assertions.assertTrue(result.hasNext());
        Assertions.assertArrayEquals(
            new Object[] {"Charlie".getBytes()}, result.next().getValues());
        Assertions.assertArrayEquals(new Object[] {"Eve".getBytes()}, result.next().getValues());
        Assertions.assertArrayEquals(new Object[] {"Frank".getBytes()}, result.next().getValues());
        Assertions.assertFalse(result.hasNext());
      }
    }
  }
}
