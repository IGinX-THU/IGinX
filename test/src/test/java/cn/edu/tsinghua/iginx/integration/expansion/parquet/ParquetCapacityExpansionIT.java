package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.PORT_TO_ROOT;
import static cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools.executeShellScript;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.parquet;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ParquetCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(ParquetCapacityExpansionIT.class);

  public ParquetCapacityExpansionIT() {
    super(parquet, "iginx_port:6888");
  }
}
