package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.influxdb;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBCapacityExpansionIT extends BaseCapacityExpansionIT {

private static final Logger logger = LoggerFactory.getLogger(InfluxDBCapacityExpansionIT.class);

public InfluxDBCapacityExpansionIT() {
    super(
        influxdb,
        "username:user, password:12345678, token:testToken, organization:testOrg",
        8086,
        8087,
        8088);
}
}
