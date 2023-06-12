package cn.edu.tsinghua.iginx.integration.expansion.influxdb;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.influxdb;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBCapacityExpansionIT extends CapacityExpansionIT {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBCapacityExpansionIT.class);

    public InfluxDBCapacityExpansionIT() {
        super(influxdb);
    }

    @Override
    public void addStorageEngineWithPrefix(String dataPrefix, String schemaPrefix) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 8087, \""
                            + influxdb
                            + "\", \"url:http://localhost:8087/ , username:user, password:12345678, sessionPoolSize:20, schema_prefix:"
                            + schemaPrefix
                            + ", data_prefix:"
                            + dataPrefix
                            + ", has_data:true, is_read_only:true, token:testToken, organization:testOrg\");");
        } catch (ExecutionException | SessionException e) {
            logger.error("add storage engine failure: {}", e.getMessage());
        }
    }

    @Override
    public void addStorageEngine(boolean hasData) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 8087, \""
                            + dbType.name()
                            + "\", \"url:http://localhost:8087/, username:user, password:12345678, sessionPoolSize:20, has_data:"
                            + hasData
                            + ", is_read_only:true, token:testToken, organization:testOrg\");");
        } catch (SessionException | ExecutionException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public int getPort() {
        return 8087;
    }
}
