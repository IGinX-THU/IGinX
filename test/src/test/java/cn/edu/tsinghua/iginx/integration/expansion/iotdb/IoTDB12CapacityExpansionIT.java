package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.iotdb12;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12CapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB12CapacityExpansionIT.class);

  public IoTDB12CapacityExpansionIT() {
    super(
        iotdb12,
        "username:root, password:root, sessionPoolSize:20",
        new IoTDB12HistoryDataGenerator());
    wrongExtraParams.add("username:root, password:wrong, sessionPoolSize:20");
    wrongExtraParams.add("username:wrong, password:root, sessionPoolSize:20");
  }
}
