package cn.edu.tsinghua.iginx.integration.expansion.redis;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.tool.DBType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisHistoryDataCapacityExpansionIT extends CapacityExpansionIT {

    private static final Logger logger =
            LoggerFactory.getLogger(RedisHistoryDataCapacityExpansionIT.class);

    public RedisHistoryDataCapacityExpansionIT() {
        super(DBType.redis);
    }

    @Override
    public void addStorageEngineWithPrefix(String dataPrefix, String schemaPrefix) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 6380, \""
                            + DBType.redis.name()
                            + "\", \"has_data:true, data_prefix:"
                            + dataPrefix
                            + ", schema_prefix:"
                            + schemaPrefix
                            + ", is_read_only:true\");");
        } catch (SessionException | ExecutionException e) {
            logger.error("add storage engine failure: {}", e.getMessage());
        }
    }

    @Override
    public void addStorageEngine(boolean hasData) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 6380, \""
                            + dbType.name()
                            + "\", \"has_data:"
                            + hasData
                            + ", is_read_only:true\");");
        } catch (SessionException | ExecutionException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public int getPort() {
        return 6380;
    }
}
