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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.integration.datasource;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.shared.MockClassGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceIT {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DataSourceIT.class);

  private IStorage storage = null;

  private final ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
  private final DBConf dbConf = conf.loadDBConf(conf.getStorageType());

  private final boolean isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);

  private IStorage getCurrentStorage() {
    String instance = dbConf.getClassName();
    try {
      Class<?> clazz = Class.forName(instance); // 获取类对象
      Constructor<?> constructor = clazz.getDeclaredConstructor(StorageEngineMeta.class);
      return (IStorage)
          constructor.newInstance(
              MockClassGenerator.genStorageEngineMetaFromConf(dbConf.getStorageEngineMockConf()));
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | InvocationTargetException
        | NoSuchMethodException e) {
      LOGGER.error("get current storage failed, caused by: ", e);
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() {
    open();
  }

  @After
  public void tearDown() throws PhysicalException {
    clear();
    close();
  }

  private void open() {
    assertNull(storage);
    storage = getCurrentStorage();
  }

  private void close() throws PhysicalException {
    storage.release();
    storage = null;
  }

  private static void checkResult(TaskExecuteResult result) {
    if (result.getException() != null) {
      LOGGER.error("execute task failed, caused by: ", result.getException());
      fail();
    }
  }

  private static void checkRowCount(RowStream rowStream, int expectedCount)
      throws PhysicalException {
    int count = 0;
    while (rowStream.hasNext()) {
      Row row = rowStream.next();
      Assert.assertNotNull(row);
      Assert.assertNotNull(row.getValues());
      for (Object value : row.getValues()) {
        Assert.assertNotNull(value);
      }
      count++;
    }
    rowStream.close();
    Assert.assertEquals(expectedCount, count);
  }

  private void clear() {
    LOGGER.info("clear data");
    TaskExecuteResult result =
        storage.executeDelete(
            new Delete(
                MockClassGenerator.genFragmentSource(),
                Collections.emptyList(),
                Collections.singletonList("*"),
                null),
            MockClassGenerator.genDataArea());
    checkResult(result);
  }

  private void assumeAbleToDelete() {
    Assume.assumeTrue(
        "delete is not supported in datasource: " + dbConf.getClassName(), isAbleToDelete);
  }

  @Test
  public void insertEmptyData() {
    DataView EmptyDataView =
        MockClassGenerator.genRowDataViewNoKey(
            0, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new Object[0]);
    Insert insert = new Insert(MockClassGenerator.genFragmentSource(), EmptyDataView);
    try {
      storage.executeInsert(insert, MockClassGenerator.genDataArea());
    } catch (Exception e) {
      LOGGER.error("insert empty body fail, caused by: ", e);
      fail();
    }
  }

  private void insertData(long start, int rows, String... paths) {
    List<String> pathList = Arrays.asList(paths);
    List<Map<String, String>> tagsList =
        Arrays.asList(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());
    List<DataType> dataTypeList =
        Arrays.asList(DataType.LONG, DataType.LONG, DataType.BINARY, DataType.DOUBLE);
    Object[] values = new Object[rows];
    for (int i = 0; i < rows; i++) {
      long key = start + (long) i;
      Object[] row = new Object[paths.length];
      for (int j = 0; j < paths.length; j++) {
        switch (j % 4) {
          case 0:
            row[j] = key;
            break;
          case 1:
            row[j] = key + 1;
            break;
          case 2:
            row[j] = ("\"" + RandomStringUtils.randomAlphabetic(10) + "\"").getBytes();
            break;
          case 3:
            row[j] = Math.random();
            break;
        }
      }
      values[i] = row;
    }

    DataView data =
        MockClassGenerator.genRowDataViewNoKey(start, pathList, tagsList, dataTypeList, values);
    Insert insert = new Insert(MockClassGenerator.genFragmentSource(), data);
    TaskExecuteResult result = storage.executeInsert(insert, MockClassGenerator.genDataArea());

    checkResult(result);
  }

  @Test
  public void insertClearInsert() throws PhysicalException {
    FragmentSource source = MockClassGenerator.genFragmentSource();
    DataArea dataArea = MockClassGenerator.genDataArea();
    Project project = new Project(source, Collections.singletonList("*"), null);

    insertData(0, 10, "us.d1.s1", "us.d1.s2", "us.d1.s3", "us.d1.s4");
    clear();
    insertData(0, 20, "us.d1.s1", "us.d1.s2", "us.d1.s3", "us.d1.s4");
    close();
    open();

    TaskExecuteResult result = storage.executeProject(project, dataArea);
    checkResult(result);
    checkRowCount(result.getRowStream(), 20);
  }

  @Test
  public void deleteClearInsert() throws PhysicalException {
    assumeAbleToDelete();

    FragmentSource source = MockClassGenerator.genFragmentSource();
    DataArea dataArea = MockClassGenerator.genDataArea();

    insertData(0, 10, "us.d1.s1");
    Delete delete =
        new Delete(
            source,
            Collections.singletonList(new KeyRange(5, 10)),
            Collections.singletonList("us.d1.*"),
            null);
    checkResult(storage.executeDelete(delete, dataArea));
    clear();
    close();
    open();
    insertData(0, 500000, "us.d1.s1");
    insertData(0, 500000, "us.d1.s2");

    Project project = new Project(source, Collections.singletonList("us.d1.s1"), null);
    TaskExecuteResult result = storage.executeProject(project, dataArea);
    checkResult(result);
    checkRowCount(result.getRowStream(), 500000);
  }

  @Test
  public void deleteRowsInMultiPart() throws PhysicalException {
    assumeAbleToDelete();

    FragmentSource source = MockClassGenerator.genFragmentSource();
    DataArea dataArea = MockClassGenerator.genDataArea();

    insertData(0, 500000, "us.d2.s2");
    insertData(0, 500005, "us.d1.s3");

    Delete delete =
        new Delete(
            source,
            Collections.singletonList(new KeyRange(5, 10)),
            Collections.singletonList("us.d1.*"),
            null);
    checkResult(storage.executeDelete(delete, dataArea));

    Project project = new Project(source, Collections.singletonList("us.d1.*"), null);
    TaskExecuteResult result = storage.executeProject(project, dataArea);
    checkResult(result);
    checkRowCount(result.getRowStream(), 500000);
  }
}
