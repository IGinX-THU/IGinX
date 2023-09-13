package cn.edu.tsinghua.iginx.integration.expansion.mongodb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.mongodb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBCapacityExpansionIT extends BaseCapacityExpansionIT {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBCapacityExpansionIT.class);

  public MongoDBCapacityExpansionIT() {
    super(mongodb, null);
    Constant.oriPort = 27017;
    Constant.expPort = 27018;
    Constant.readOnlyPort = 27019;
  }
}
