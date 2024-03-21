package cn.edu.tsinghua.iginx.integration.datasource;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.shared.MockClassGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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

  public DataSourceIT() {}

  @Before
  public void init() {
    storage = getCurrentStorage(new ConfLoader(Controller.CONFIG_FILE));
  }

  @After
  public void clean() throws PhysicalException {
    Delete delete =
        new Delete(
            MockClassGenerator.genFragmentSource(),
            Collections.emptyList(),
            Collections.singletonList("*"),
            null);
    storage.executeDelete(delete, MockClassGenerator.genDataArea());
    storage.release();
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
      logger.error("insert empty body fail, caused by: {}", e.getMessage());
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void concurrentInsertBigDataset() throws InterruptedException {
    AtomicLong counter = new AtomicLong(0);
    int pathNum = 10;
    int rowNum = 1000;
    int threadNum = 10;
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < threadNum; i++) {
      Thread thread =
          new Thread(
              () -> {
                for (int j = 0; j < 100; j++) {
                  DataView dataView =
                      genRandomBinaryDataView(counter.getAndAdd(rowNum), pathNum, rowNum);
                  Insert insert = new Insert(MockClassGenerator.genFragmentSource(), dataView);
                  try {
                    storage.executeInsert(insert, MockClassGenerator.genDataArea());
                  } catch (Throwable e) {
                    logger.error("concurrent insert big dataset fail, caused by: ", e);
                    fail();
                  }
                }
              });
      threads.add(thread);
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }

  private static DataView genRandomBinaryDataView(long startKey, int pathNum, int rowNum) {
    List<String> pathList = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();
    List<DataType> dataTypeList = new ArrayList<>();
    Object[] valuesList = new Object[rowNum];
    for (int i = 0; i < pathNum; i++) {
      pathList.add("test.path" + i);
      tagsList.add(Collections.emptyMap());
      dataTypeList.add(DataType.BINARY);
    }
    for (int i = 0; i < rowNum; i++) {
      Object[] values = new Object[pathNum];
      for (int j = 0; j < pathNum; j++) {
        // generate random string
        StringBuilder sb = new StringBuilder();
        int randomLength = (int) (Math.random() * 90) + 10;
        for (int k = 0; k < randomLength; k++) {
          sb.append((char) (Math.random() * 26 + 'a'));
        }
        values[j] = sb.toString().getBytes();
      }
      valuesList[i] = values;
    }
    return MockClassGenerator.genRowDataView(
        pathList, tagsList, dataTypeList, valuesList, startKey);
  }
}
