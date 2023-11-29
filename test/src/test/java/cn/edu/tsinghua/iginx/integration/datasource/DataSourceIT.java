package cn.edu.tsinghua.iginx.integration.datasource;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.shared.EmptyClassGenerator;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
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

  private final Map<String, StorageEngineMeta> NAME_TO_META =
      new HashMap<String, StorageEngineMeta>() {
        {
          // filesystem
          put(
              "FileSystem",
              new StorageEngineMeta(
                  0,
                  "127.0.0.1",
                  6667,
                  new HashMap<String, String>() {
                    {
                      put("dir", "test/iginx_mn");
                      put("iginx_port", "6888");
                    }
                  },
                  StorageEngineType.filesystem,
                  0));
          // iotdb
          put(
              "IoTDB12",
              new StorageEngineMeta(
                  0,
                  "127.0.0.1",
                  6667,
                  new HashMap<String, String>() {
                    {
                      put("username", "root");
                      put("password", "root");
                    }
                  },
                  StorageEngineType.iotdb12,
                  0));
          // influxdb
          put(
              "InfluxDB",
              new StorageEngineMeta(
                  0,
                  "127.0.0.1",
                  8086,
                  new HashMap<String, String>() {
                    {
                      put("url", "http://localhost:8086/");
                      put("token", "testToken");
                      put("organization", "testOrg");
                    }
                  },
                  StorageEngineType.influxdb,
                  0));
          // postgresql
          put(
              "PostgreSQL",
              new StorageEngineMeta(
                  0,
                  "127.0.0.1",
                  5432,
                  new HashMap<String, String>() {
                    {
                      put("username", "postgres");
                      put("password", "postgres");
                    }
                  },
                  StorageEngineType.postgresql,
                  0));
          // redis
          put(
              "Redis",
              new StorageEngineMeta(
                  0,
                  "127.0.0.1",
                  6379,
                  new HashMap<String, String>() {
                    {
                      put("timeout", "5000");
                    }
                  },
                  StorageEngineType.redis,
                  0));
          // mongodb
          put(
              "MongoDB",
              new StorageEngineMeta(
                  0, "127.0.0.1", 27017, new HashMap<>(), StorageEngineType.mongodb, 0));
          // parquet
          put(
              "Parquet",
              new StorageEngineMeta(
                  0,
                  "127.0.0.1",
                  6667,
                  new HashMap<String, String>() {
                    {
                      put("dir", "test/iginx_mn");
                      put("iginx_port", "6888");
                    }
                  },
                  StorageEngineType.parquet,
                  0));
        }
      };

  private IStorage getCurrentStorage(ConfLoader conf) {
    String instance = NAME_TO_INSTANCE.get(conf.getStorageType());
    try {
      Class<?> clazz = Class.forName(instance); // 获取类对象
      Constructor<?> constructor = clazz.getDeclaredConstructor(StorageEngineMeta.class);
      return (IStorage) constructor.newInstance(NAME_TO_META.get(conf.getStorageType()));
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
        EmptyClassGenerator.genRowDataViewNoKey(
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new Object[0]);
    Insert insert = new Insert(EmptyClassGenerator.genFragmentSource(), EmptyDataView);
    try {
      storage.executeInsert(insert, EmptyClassGenerator.genDataArea());
      storage.release();
    } catch (Exception e) {
      logger.error("insert empty body fail, caused by: {}", e.getMessage());
      e.printStackTrace();
      fail();
    }
  }
}
