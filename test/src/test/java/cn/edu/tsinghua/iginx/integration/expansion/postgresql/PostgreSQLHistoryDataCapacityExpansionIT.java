package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;

public class PostgreSQLHistoryDataCapacityExpansionIT extends CapacityExpansionIT {
    public PostgreSQLHistoryDataCapacityExpansionIT() {
        super("postgresql");
    }

    @Override
    protected void addStorageWithPrefix(String dataPrefix, String schemaPrefix) throws Exception {
        session.executeSql(
                "ADD STORAGEENGINE (\"127.0.0.1\", 5433, \""
                        + ENGINE_TYPE
                        + "\", \"username:postgres, password:postgres, has_data:true, data_prefix:"
                        + dataPrefix
                        + ", schema_prefix:"
                        + schemaPrefix
                        + ", is_read_only:true\");");
    }

    @Override
    protected int getPort() throws Exception {
        return 5433;
    }
}
