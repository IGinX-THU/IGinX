package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.AbstractServiceTest;
import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import com.typesafe.config.Config;
import org.apache.thrift.transport.TTransportException;

import java.nio.file.Paths;
import java.util.UUID;

public abstract class AbstractStorageServiceTest extends AbstractServiceTest {

  private final StorageConfig config;
  private final DataUnit dataUnit;

  protected AbstractStorageServiceTest(String type, Config config) {
    String root = Paths.get("target", "test", UUID.randomUUID().toString()).toString();
    this.config = new StorageConfig(root, type, config);
    this.dataUnit = new DataUnit(false);
    this.dataUnit.setName("test0001");
  }

  private Service service;

  @Override
  protected Service getService() throws Exception {
    if (service == null) {
      service = new StorageService(config, null);
    }
    return service;
  }

  @Override
  protected DataUnit getUnit() {
    return dataUnit;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (service != null) {
      service.close();
      service = null;
    }
  }

}
