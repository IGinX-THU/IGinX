package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import cn.edu.tsinghua.iginx.integration.expansion.CapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.unit.SQLTestTools;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IoTDB12HistoryDataCapacityExpansionIT extends CapacityExpansionIT {
    public IoTDB12HistoryDataCapacityExpansionIT() {
        super("iotdb12");
    }
    
    @Override
    protected void addStorageWithPrefix(String dataPrefix, String schemaPrefix) throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, data_prefix:" + dataPrefix + ", schema_prefix:" + schemaPrefix + ", is_read_only:true\");");
    }

    @Override
    protected int getPort() throws Exception {
        return 6668;
    }
}