package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.postgresql;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLCapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCapacityExpansionIT.class);

  public PostgreSQLCapacityExpansionIT() {
    super(postgresql, "username:postgres, password:postgres", 5432, 5433, 5434);
  }
}
