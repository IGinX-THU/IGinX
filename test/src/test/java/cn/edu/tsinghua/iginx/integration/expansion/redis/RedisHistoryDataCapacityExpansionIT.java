package cn.edu.tsinghua.iginx.integration.expansion.redis;

import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;

public class RedisHistoryDataCapacityExpansionIT extends CapacityExpansionIT {

    public RedisHistoryDataCapacityExpansionIT() {
        super("redis");
    }

    @Override
    protected void addStorageWithPrefix(String dataPrefix, String schemaPrefix) throws Exception {
        session.executeSql(
                "ADD STORAGEENGINE (\"127.0.0.1\", 6380, \""
                        + ENGINE_TYPE
                        + "\", \"has_data:true, data_prefix:"
                        + dataPrefix
                        + ", schema_prefix:"
                        + schemaPrefix
                        + ", is_read_only:true\");");
    }

    @Override
    protected int getPort() throws Exception {
        return 6380;
    }
}
