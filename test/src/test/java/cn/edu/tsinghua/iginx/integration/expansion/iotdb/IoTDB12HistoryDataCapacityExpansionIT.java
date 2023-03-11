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

    @Test
    public void schemaPrefix() throws Exception {
        testSchemaPrefix();
    }

    @Test
    public void testSchemaPrefix() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, schema_prefix:expansion, is_read_only:true\");");

        String statement = "select * from expansion.ln.wf03";
        String expect = "ResultSets:\n" +
                "+---+-----------------------------+----------------------------------+\n" +
                "|key|expansion.ln.wf03.wt01.status|expansion.ln.wf03.wt01.temperature|\n" +
                "+---+-----------------------------+----------------------------------+\n" +
                "| 77|                         true|                              null|\n" +
                "|200|                        false|                             77.71|\n" +
                "+---+-----------------------------+----------------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    @Test
    public void testDataPrefix() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, data_prefix:test, is_read_only:true\");");

        String statement = "select * from test";
        String expect = "ResultSets:\n" +
                "+---+---------------------+--------------------------+\n" +
                "|key|test.wf03.wt01.status|test.wf03.wt01.temperature|\n" +
                "+---+---------------------+--------------------------+\n" +
                "| 77|                 true|                      null|\n" +
                "|200|                false|                     77.71|\n" +
                "+---+---------------------+--------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    @Test
    public void testAddSameDataPrefixWithDiffSchemaPrefix_AND_testRemoveHistoryDataSource() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, data_prefix:test, schema_prefix:p1, is_read_only:true\");");
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, data_prefix:test, schema_prefix:p2, is_read_only:true\");");

        String statement = "select * from p1.test";
        String expect = "ResultSets:\n" +
                "+---+------------------------+-----------------------------+\n" +
                "|key|p1.test.wf03.wt01.status|p1.test.wf03.wt01.temperature|\n" +
                "+---+------------------------+-----------------------------+\n" +
                "| 77|                    true|                         null|\n" +
                "|200|                   false|                        77.71|\n" +
                "+---+------------------------+-----------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from p2.test";
        expect = "ResultSets:\n" +
                "+---+------------------------+-----------------------------+\n" +
                "|key|p2.test.wf03.wt01.status|p2.test.wf03.wt01.temperature|\n" +
                "+---+------------------------+-----------------------------+\n" +
                "| 77|                    true|                         null|\n" +
                "|200|                   false|                        77.71|\n" +
                "+---+------------------------+-----------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from test";
        expect = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, data_prefix:test, is_read_only:false\");");
        statement = "select * from test";
        expect = "ResultSets:\n" +
                "+---+---------------------+--------------------------+\n" +
                "|key|test.wf03.wt01.status|test.wf03.wt01.temperature|\n" +
                "+---+---------------------+--------------------------+\n" +
                "| 77|                 true|                      null|\n" +
                "|200|                false|                     77.71|\n" +
                "+---+---------------------+--------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
        removedStorageEngineList.add(new RemovedStorageEngineInfo("127.0.0.1", 6668, "", "test"));
        session.removeHistoryDataSource(removedStorageEngineList);
        statement = "select * from test";
        expect = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        removedStorageEngineList.set(0, new RemovedStorageEngineInfo("127.0.0.1", 6668, "p2", "test"));
        sessionPool.removeHistoryDataSource(removedStorageEngineList);
        statement = "select * from p2.test";
        expect = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        session.executeSql("remove historydataresource (\"127.0.0.1\", 6668, \"p1\", \"test\")");
        statement = "select * from p1.test";
        expect = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }


}
