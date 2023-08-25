package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.parquet;

public class ParquetCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(ParquetCapacityExpansionIT.class);

  public ParquetCapacityExpansionIT() {
    super(parquet, "iginx_port:6888", 6667, 6668, 6669);
  }
}
