package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import static cn.edu.tsinghua.iginx.integration.tool.DBType.iotdb12;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12CapacityExpansionIT extends CapacityExpansionIT {

    private static final Logger logger = LoggerFactory.getLogger(IoTDB12CapacityExpansionIT.class);

    public IoTDB12CapacityExpansionIT() {
        super(iotdb12);
    }

    @Override
    public void addStorageEngineWithPrefix(String dataPrefix, String schemaPrefix) {
        try {
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 6668, \""
                            + iotdb12.name()
                            + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, data_prefix:"
                            + dataPrefix
                            + ", schema_prefix:"
                            + schemaPrefix
                            + ", is_read_only:true\");");
        } catch (ExecutionException | SessionException e) {
            logger.error("add storage engine failure: {}", e.getMessage());
        }
    }

    @Override
    public int getPort() {
        return 6668;
    }
}
