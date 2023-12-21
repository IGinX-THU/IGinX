package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.parquet;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFormatCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger =
      LoggerFactory.getLogger(ParquetFormatCapacityExpansionIT.class);

  public ParquetFormatCapacityExpansionIT() {
    super(parquet, null);
  }
}
