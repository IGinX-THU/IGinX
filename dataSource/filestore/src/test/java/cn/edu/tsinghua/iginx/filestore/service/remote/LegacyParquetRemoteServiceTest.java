package cn.edu.tsinghua.iginx.filestore.service.remote;

import cn.edu.tsinghua.iginx.filestore.service.storage.AbstractStorageServiceTest;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.LegacyParquet;
import com.typesafe.config.ConfigFactory;

public class LegacyParquetRemoteServiceTest extends AbstractRemoteServiceTest {

  public LegacyParquetRemoteServiceTest() {
    super(LegacyParquet.NAME, ConfigFactory.empty());
  }

}
