package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.postgresql;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCapacityExpansionIT.class);

  public PostgreSQLCapacityExpansionIT() {
    super(postgresql, "username:postgres, password:postgres");
    Constant.oriPort = 5432;
    Constant.expPort = 5433;
    Constant.readOnlyPort = 5434;
  }
}
