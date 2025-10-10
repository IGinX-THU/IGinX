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
package cn.edu.tsinghua.iginx.integration.distributed.restart;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartIT {

  protected static final Logger LOGGER = LoggerFactory.getLogger(RestartIT.class);

  protected static String SELECT_ALL = "select * from *;";

  protected final Session session6888 = new Session("127.0.0.1", 6888);

  protected final Session session6889 = new Session("127.0.0.1", 6889);

  @Test
  public void prepareDataForIGinX6888() {
    try {
      session6888.openSession();
      testShowReplicaNumber(session6888);

      // z.g的数据在filesystem6668和iotdb上
      String insert = "insert into z(key, g) values (0, 0), (1, 10), (2, 20), (3, 30);";
      testInsert(session6888, insert, true, "");

      String expected =
          "ResultSets:\n"
              + "+---+---+\n"
              + "|key|z.g|\n"
              + "+---+---+\n"
              + "|  0|  0|\n"
              + "|  1| 10|\n"
              + "|  2| 20|\n"
              + "|  3| 30|\n"
              + "+---+---+\n"
              + "Total line number = 4\n";
      testSelect(session6888, expected);
    } catch (SessionException e) {
      LOGGER.error("fail to open session, caused by: ", e);
      fail();
    } finally {
      try {
        session6888.closeSession();
      } catch (SessionException e) {
        LOGGER.error("fail to close session, caused by: ", e);
      }
    }
  }

  @Test
  public void prepareDataForIGinX6889() {
    try {
      session6889.openSession();
      testShowReplicaNumber(session6889);

      // a.m的数据在filesystem6669和iotdb上
      String insert = "insert into a(key, m) values (0, 0), (1, 10), (2, 100);";
      testInsert(session6889, insert, true, "");

      String expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|null| 30|\n"
              + "+---+----+---+\n"
              + "Total line number = 4\n";
      testSelect(session6889, expected);
    } catch (SessionException e) {
      LOGGER.error("fail to open session, caused by: ", e);
      fail();
    } finally {
      try {
        session6889.closeSession();
      } catch (SessionException e) {
        LOGGER.error("fail to close session, caused by: ", e);
      }
    }
  }

  @Test
  public void testRestartIGinX6888() {
    try {
      session6888.openSession();
      testShowReplicaNumber(session6888);
      testStorageConnectivity(session6888, Arrays.asList("true", "true", "false"));

      String expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|null| 30|\n"
              + "+---+----+---+\n"
              + "Total line number = 4\n";
      testSelect(session6888, expected);

      String insert = "insert into z(key, g) values (4, 40);";
      testInsert(session6888, insert, true, "");

      expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|null| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6888, expected);

      // filesystem6669未启动，数据写入失败
      insert = "insert into a(key, m) values (3, 1000);";
      testInsert(
          session6888,
          insert,
          false,
          "some sub-task execute failure, details: Insert task failed for unconnected storage: 2");

      expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|null| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6888, expected);
    } catch (SessionException e) {
      LOGGER.error("fail to open session, caused by: ", e);
      fail();
    } finally {
      try {
        session6888.closeSession();
      } catch (SessionException e) {
        LOGGER.error("fail to close session, caused by: ", e);
      }
    }
  }

  @Test
  public void testRestartIGinX6889() {
    try {
      session6889.openSession();
      testShowReplicaNumber(session6889);
      testStorageConnectivity(session6889, Arrays.asList("false", "true", "true"));

      String expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|null| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6889, expected);

      String insert = "insert into a(key, m) values (3, 1000);";
      testInsert(session6889, insert, true, "");

      expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|1000| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6889, expected);

      // filesystem6668未启动，数据写入失败
      insert = "insert into z(key, g) values (4, 40);";
      testInsert(
          session6889,
          insert,
          false,
          "some sub-task execute failure, details: Insert task failed for unconnected storage: 0");

      expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|1000| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6889, expected);

    } catch (SessionException e) {
      LOGGER.error("fail to open session, caused by: ", e);
      fail();
    } finally {
      try {
        session6889.closeSession();
      } catch (SessionException e) {
        LOGGER.error("fail to close session, caused by: ", e);
      }
    }
  }

  @Test
  public void testRestartIGinX6888And6889() {
    try {
      session6888.openSession();
      session6889.openSession();
      testShowReplicaNumber(session6888);
      testShowReplicaNumber(session6889);
      testStorageConnectivity(session6888, Arrays.asList("true", "true", "true"));
      testStorageConnectivity(session6889, Arrays.asList("true", "true", "true"));

      String expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|1000| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6888, expected);

      expected =
          "ResultSets:\n"
              + "+---+----+---+\n"
              + "|key| a.m|z.g|\n"
              + "+---+----+---+\n"
              + "|  0|   0|  0|\n"
              + "|  1|  10| 10|\n"
              + "|  2| 100| 20|\n"
              + "|  3|1000| 30|\n"
              + "|  4|null| 40|\n"
              + "+---+----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6889, expected);

      String insert = "insert into a(key, m) values (4, 10000);";
      testInsert(session6889, insert, true, "");

      expected =
          "ResultSets:\n"
              + "+---+-----+---+\n"
              + "|key|  a.m|z.g|\n"
              + "+---+-----+---+\n"
              + "|  0|    0|  0|\n"
              + "|  1|   10| 10|\n"
              + "|  2|  100| 20|\n"
              + "|  3| 1000| 30|\n"
              + "|  4|10000| 40|\n"
              + "+---+-----+---+\n"
              + "Total line number = 5\n";
      testSelect(session6888, expected);

    } catch (SessionException e) {
      LOGGER.error("fail to open session, caused by: ", e);
      fail();
    } finally {
      try {
        session6888.closeSession();
        session6889.closeSession();
      } catch (SessionException e) {
        LOGGER.error("fail to close session, caused by: ", e);
      }
    }
  }

  private void testShowReplicaNumber(Session session) {
    try {
      int replicaNumber = session.getReplicaNum();
      assertEquals(2, replicaNumber);
    } catch (SessionException e) {
      LOGGER.error("fail to show replica number, caused by: ", e);
      fail();
    }
  }

  private void testStorageConnectivity(Session session, List<String> expected) {
    try {
      List<StorageEngineInfo> storages = session.getClusterInfo().getStorageEngineInfos();
      assertEquals(
          expected,
          storages.stream().map(StorageEngineInfo::getConnectable).collect(Collectors.toList()));
    } catch (SessionException e) {
      LOGGER.error("fail to show cluster info, caused by: ", e);
      fail();
    }
  }

  private void testInsert(
      Session session, String insert, boolean expectSuccess, String expectedMessage) {
    try {
      session.executeSql(insert);
      assertTrue(expectSuccess);
    } catch (SessionException e) {
      assertFalse(expectSuccess);
      assertEquals(expectedMessage, e.getMessage());
    }
  }

  private void testSelect(Session session, String expected) {
    try {
      SessionExecuteSqlResult res = session.executeSql(SELECT_ALL);
      assertEquals(expected, res.getResultInString(false, ""));
    } catch (SessionException e) {
      LOGGER.error("fail to execute {}, caused by: ", SELECT_ALL, e);
    }
  }
}
