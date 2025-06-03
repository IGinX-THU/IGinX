/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.integration.distributed.startup;

import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.iotdb.IoTDB12HistoryDataGenerator;
import cn.edu.tsinghua.iginx.session.ClusterInfo;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.IginxInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterIT {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterIT.class);

  protected static String SHOW_CLUSTER_INFO = "SHOW CLUSTER INFO;";

  protected final Session session6888 = new Session("127.0.0.1", 6888);

  protected final Session session6889 = new Session("127.0.0.1", 6889);

  protected final Session session6890 = new Session("127.0.0.1", 6890);

  @Before
  public void setUp() throws SessionException, IOException, ParseException {
    session6888.openSession();
    session6889.openSession();
    session6890.openSession();
  }

  @After
  public void tearDown() throws SessionException {
    session6888.closeSession();
    session6889.closeSession();
    session6890.closeSession();
  }

  @Test
  public void testIginxConnectivity() {
    testIginxConnectivity(session6888);
    testIginxConnectivity(session6889);
    testIginxConnectivity(session6890);
  }

  private void testIginxConnectivity(Session session) {
    try {
      LOGGER.info(
          "Execute SHOW CLUSTER INFO for session (host: {}, port: {})",
          session.getHost(),
          session.getPort());
      ClusterInfo clusterInfo = session.getClusterInfo();
      assertEquals(3, clusterInfo.getIginxInfos().size());
      for (IginxInfo iginxInfo : clusterInfo.getIginxInfos()) {
        if (iginxInfo.getPort() == session.getPort()) {
          assertEquals("self", iginxInfo.getConnectable());
        } else {
          assertEquals("true", iginxInfo.getConnectable());
        }
      }
      SessionExecuteSqlResult res = session.executeSql(SHOW_CLUSTER_INFO);
      String result = res.getResultInString(false, "");
      LOGGER.info("Result: \"{}\"", result);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"SHOW CLUSTER INFO;\" execute fail. Caused by: ", e);
      fail();
    }
  }

  @Test
  public void testRemoveDummyStorageForCurrentIginx() {
    IoTDB12HistoryDataGenerator generator = new IoTDB12HistoryDataGenerator();
    try {
      generator.writeHistoryData(
          6667,
          Collections.singletonList("test.a"),
          Collections.singletonList(LONG),
          Collections.singletonList(11L),
          Collections.singletonList(Collections.singletonList(111L)));

      addStorageEngine();
      testRemoveDummyStorageForCurrentIginx(session6888);
      testRemoveDummyStorageForCurrentIginx(session6889);
      testRemoveDummyStorageForCurrentIginx(session6890);
    } finally {
      generator.clearHistoryDataForGivenPort(6667);
    }
  }

  @Test
  public void testRemoveDummyStorageForAllIginx() {
    IoTDB12HistoryDataGenerator generator = new IoTDB12HistoryDataGenerator();
    try {
      generator.writeHistoryData(
          6667,
          Collections.singletonList("test.a"),
          Collections.singletonList(LONG),
          Collections.singletonList(11L),
          Collections.singletonList(Collections.singletonList(111L)));

      addStorageEngine();
      testRemoveDummyStorageForAllIginx(session6888);

      addStorageEngine();
      testRemoveDummyStorageForAllIginx(session6889);

      addStorageEngine();
      testRemoveDummyStorageForAllIginx(session6890);
    } finally {
      generator.clearHistoryDataForGivenPort(6667);
    }
  }

  private void addStorageEngine() {
    String addStorageEngine =
        "ADD STORAGEENGINE (\"127.0.0.1\", 6667, \"iotdb12\", \"has_data=true, is_read_only=true, username=root, sessionPoolSize=20, schema_prefix=prefix\");";
    try {
      LOGGER.info("Execute Statement: \"{}\"", addStorageEngine);
      session6888.executeSql(addStorageEngine);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", addStorageEngine, e);
      fail();
    }

    SessionExecuteSqlResult res = null;
    try {
      res = session6888.executeSql(SHOW_CLUSTER_INFO);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"SHOW CLUSTER INFO;\" execute fail. Caused by: ", e);
      fail();
    }
    String result = res.getResultInString(false, "");
    LOGGER.info("SHOW CLUSTER INFO: \"{}\"", result);
  }

  private void testRemoveDummyStorageForCurrentIginx(Session session) {
    testQuery(session, false);
    testShowStorageConnectivity(session, true, false);

    String removeStorageEngine = "REMOVE STORAGEENGINE (\"127.0.0.1\", 6667, \"prefix\", \"\");";
    try {
      session.executeSql(removeStorageEngine);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", removeStorageEngine, e);
      fail();
    }

    testQuery(session, true);
    testShowStorageConnectivity(session, false, false);
  }

  private void testRemoveDummyStorageForAllIginx(Session session) {
    String removeStorageEngine = "REMOVE STORAGEENGINE (\"127.0.0.1\", 6667, \"prefix\", \"\") FOR ALL;";
    try {
      session.executeSql(removeStorageEngine);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", removeStorageEngine, e);
      fail();
    }

    testQuery(session6888, true);
    testShowStorageConnectivity(session6888, false, true);
    testQuery(session6889, true);
    testShowStorageConnectivity(session6889, false, true);
    testQuery(session6890, true);
    testShowStorageConnectivity(session6890, false, true);
  }

  private void testQuery(Session session, boolean afterRemove) {
    String query = "select test.a from prefix;";
    String expected;
    if (afterRemove) {
      expected = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    } else {
      expected =
          "ResultSets:\n"
              + "+---+-------------+\n"
              + "|key|prefix.test.a|\n"
              + "+---+-------------+\n"
              + "| 11|          111|\n"
              + "+---+-------------+\n"
              + "Total line number = 1\n";
    }
    try {
      LOGGER.info("Execute Statement: \"{}\"", query);
      SessionExecuteSqlResult res = session.executeSql(query);
      String result = res.getResultInString(false, "");
      assertEquals(expected, result);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", query, e);
      fail();
    }
  }

  private void testShowStorageConnectivity(Session session, boolean connectable, boolean notShow) {
    try {
      LOGGER.info(
          "Execute SHOW CLUSTER INFO for session (host: {}, port: {})",
          session.getHost(),
          session.getPort());
      List<StorageEngineInfo> storages = session.getClusterInfo().getStorageEngineInfos();
      List<StorageEngineInfo> expected =
          storages.stream()
              .filter(
                  storageEngineInfo ->
                      storageEngineInfo.getType().equals(StorageEngineType.iotdb12)
                          && storageEngineInfo.getIp().equals("127.0.0.1")
                          && storageEngineInfo.getPort() == 6667
                          && storageEngineInfo.getSchemaPrefix().equals("prefix")
                          && storageEngineInfo.getDataPrefix().isEmpty())
              .collect(Collectors.toList());
      if (notShow) {
        assertEquals(0, expected.size());
      } else if (connectable) {
        assertEquals(1, expected.size());
        assertEquals("true", expected.get(0).getConnectable());
      } else {
        assertEquals(1, expected.size());
        assertEquals("false", expected.get(0).getConnectable());
      }
      SessionExecuteSqlResult res = session.executeSql(SHOW_CLUSTER_INFO);
      String result = res.getResultInString(false, "");
      LOGGER.info("Result: \"{}\"", result);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"SHOW CLUSTER INFO;\" execute fail. Caused by: ", e);
      fail();
    }
  }
}
