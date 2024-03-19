package cn.edu.tsinghua.iginx.integration.datasource;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceIT {
  protected static final Logger logger = LoggerFactory.getLogger(DataSourceIT.class);

  private IStorage storage = null;

  private IStorage getCurrentStorage(ConfLoader conf) {
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
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
      logger.error("get current storage failed, caused by: {}", e.getMessage());
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
    storage = getCurrentStorage(new ConfLoader(Controller.CONFIG_FILE));
  }

  private void close() throws PhysicalException {
    storage.release();
    storage = null;
  }

  private static void checkResult(TaskExecuteResult result) {
    if (result.getException() != null) {
      logger.error("execute task failed, caused by: ", result.getException());
      fail();
    }
  }

  private void clear() {
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

  @Test
  public void insertEmptyBody() {
    DataView EmptyDataView =
        MockClassGenerator.genRowDataViewNoKey(
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new Object[0]);
    Insert insert = new Insert(MockClassGenerator.genFragmentSource(), EmptyDataView);
    try {
      storage.executeInsert(insert, MockClassGenerator.genDataArea());
    } catch (Exception e) {
      logger.error("insert empty body fail, caused by: ", e);
      fail();
    }
  }

  private void insertData(int rows) {
    List<String> pathList = Arrays.asList("us.d1.s1", "us.d1.s2", "us.d1.s3", "us.d1.s4");
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
      values[i] =
          new Object[] {
            (long) i,
            (long) i + 1,
            ("\"" + RandomStringUtils.randomAlphanumeric(10) + "\"").getBytes(),
            (i + 0.1d)
          };
    }

    DataView data =
        MockClassGenerator.genRowDataViewNoKey(pathList, tagsList, dataTypeList, values);
    Insert insert = new Insert(MockClassGenerator.genFragmentSource(), data);
    TaskExecuteResult result = storage.executeInsert(insert, MockClassGenerator.genDataArea());
    checkResult(result);
  }

  @Test
  public void insertCleanInsertReopenRead() throws PhysicalException {
    FragmentSource source = MockClassGenerator.genFragmentSource();
    DataArea dataArea = MockClassGenerator.genDataArea();
    Project project = new Project(source, Collections.singletonList("*"), null);

    insertData(10);
    clear();
    insertData(20);
    close();
    open();

    TaskExecuteResult result = storage.executeProject(project, dataArea);
    checkResult(result);

    RowStream rowStream = result.getRowStream();
    int count = 0;
    while (rowStream.hasNext()) {
      rowStream.next();
      count++;
    }
    rowStream.close();
    if (count != 20) {
      fail();
    }
  }
}
