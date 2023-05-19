package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBHistoryDataCapacityExpansionIT extends CapacityExpansionIT {
    private static final Logger logger =
            LoggerFactory.getLogger(InfluxDBHistoryDataCapacityExpansionIT.class);

    public InfluxDBHistoryDataCapacityExpansionIT() {
        super("influxdb");
    }

    @Override
    protected void addStorageWithPrefix(String dataPrefix, String schemaPrefix) throws Exception {
        session.executeSql(
                "ADD STORAGEENGINE (\"127.0.0.1\", 8060, \""
                        + ENGINE_TYPE
                        + "\", \"url:http://localhost:8087/ , username:user, password:12345678, sessionPoolSize:20, schema_prefix:"
                        + schemaPrefix
                        + ", data_prefix:"
                        + dataPrefix
                        + ", has_data:true, is_read_only:true, token:testToken, organization:testOrg\");");
    }

    @Override
    protected int getPort() throws Exception {
        return 8060;
    }
}
