package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.postgresql;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLCapacityExpansionIT extends CapacityExpansionIT {

    private static final Logger logger =
            LoggerFactory.getLogger(PostgreSQLCapacityExpansionIT.class);

    public PostgreSQLCapacityExpansionIT() {
        super(postgresql);
    }

    @Override
    public void addStorageEngineWithPrefix(String dataPrefix, String schemaPrefix) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 5433, \""
                            + postgresql.name()
                            + "\", \"username:postgres, password:postgres, has_data:true, data_prefix:"
                            + dataPrefix
                            + ", schema_prefix:"
                            + schemaPrefix
                            + ", is_read_only:true\");");
        } catch (ExecutionException | SessionException e) {
            logger.error("add storage engine with prefix failure: {}", e.getMessage());
        }
    }

    @Override
    public void addStorageEngine(boolean hasData) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 5431, \""
                            + dbType.name()
                            + "\", \"username:postgres, password:postgres, has_data:"
                            + hasData
                            + ", is_read_only:true\");");
        } catch (ExecutionException | SessionException e) {
            logger.error("add storage engine failure: {}", e.getMessage());
        }
    }

    @Override
    public int getPort() {
        return 5433;
    }
}
