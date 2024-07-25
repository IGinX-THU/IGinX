package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.LegacyParquet;
import com.typesafe.config.ConfigFactory;

public class LegacyParquetHistoryDataTest extends AbstractHistoryDataTest {

  public LegacyParquetHistoryDataTest() {
    super(LegacyParquet.NAME, ConfigFactory.empty(), ConfigFactory.empty());
  }
}
