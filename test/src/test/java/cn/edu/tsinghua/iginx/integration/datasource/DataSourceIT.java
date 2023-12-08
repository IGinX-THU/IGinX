package cn.edu.tsinghua.iginx.integration.datasource;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.shared.MockClassGenerator;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceIT {
  protected static final Logger logger = LoggerFactory.getLogger(DataSourceIT.class);

  private IStorage storage = null;
  private final Map<String, String> NAME_TO_INSTANCE =
      new HashMap<String, String>() {
        {
          put("FileSystem", "cn.edu.tsinghua.iginx.filesystem.FileSystemStorage");
          put("IoTDB12", "cn.edu.tsinghua.iginx.iotdb.IoTDBStorage");
          put("InfluxDB", "cn.edu.tsinghua.iginx.influxdb.InfluxDBStorage");
          put("PostgreSQL", "cn.edu.tsinghua.iginx.postgresql.PostgreSQLStorage");
          put("Redis", "cn.edu.tsinghua.iginx.redis.RedisStorage");
          put("MongoDB", "cn.edu.tsinghua.iginx.mongodb.MongoDBStorage");
          put("Parquet", "cn.edu.tsinghua.iginx.parquet.ParquetStorage");
        }
      };

  private IStorage getCurrentStorage(ConfLoader conf) {
    String instance = NAME_TO_INSTANCE.get(conf.getStorageType());
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
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

  public DataSourceIT() {
    storage = getCurrentStorage(new ConfLoader(Controller.CONFIG_FILE));
  }

  @Test
  public void insertEmptyBody() {
    DataView EmptyDataView =
        MockClassGenerator.genRowDataViewNoKey(
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new Object[0]);
    Insert insert = new Insert(MockClassGenerator.genFragmentSource(), EmptyDataView);
    try {
      storage.executeInsert(insert, MockClassGenerator.genDataArea());
      storage.release();
    } catch (Exception e) {
      logger.error("insert empty body fail, caused by: {}", e.getMessage());
      e.printStackTrace();
      fail();
    }
  }
}
