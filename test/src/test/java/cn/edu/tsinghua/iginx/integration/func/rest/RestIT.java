/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.integration.func.rest;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.util.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestIT.class);

  private boolean isAbleToClearData = true;

  private static Session session;

  private boolean isAbleToDelete = true;

  // dummy节点是够已经插入过数据
  private boolean isDummyHasInitialData = false;

  public RestIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    this.isAbleToClearData = dbConf.getEnumValue(DBConf.DBConfType.isAbleToClearData);
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }

  @Before
  public void insertData() {
    List<String> pathList = Arrays.asList("archive.file.tracked", "archive.file.search");
    List<DataType> dataTypeList = Arrays.asList(DataType.DOUBLE, DataType.DOUBLE);

    List<List<Long>> keyList =
        Arrays.asList(
            new ArrayList<Long>() {
              {
                add(1359788300000L);
                add(1359788400000L);
                add(1359788410000L);
              }
            },
            new ArrayList<Long>() {
              {
                add(1359786400000L);
              }
            });
    List<List<Object>> valuesList =
        Arrays.asList(
            new ArrayList<Object>() {
              {
                add(13.2);
                add(123.3);
                add(23.1);
              }
            },
            new ArrayList<Object>() {
              {
                add(321.0);
              }
            });
    List<Map<String, String>> tagsList =
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("dc", "DC1");
                put("host", "server1");
              }
            },
            new HashMap<String, String>() {
              {
                put("host", "server2");
              }
            });
    Controller.writeColumnsData(
        session,
        pathList,
        keyList,
        dataTypeList,
        valuesList,
        tagsList,
        InsertAPIType.Column,
        isDummyHasInitialData);
    isDummyHasInitialData = false;
    Controller.after(session);
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  public enum TYPE {
    QUERY,
    INSERT,
    DELETE,
    DELETE_METRIC
  }

  public String orderGen(String json, TYPE type) {
    StringBuilder ret = new StringBuilder();
    if (type.equals(TYPE.DELETE_METRIC)) {
      ret.append("curl -XDELETE");
      ret.append(" http://127.0.0.1:6666/api/v1/metric/{");
      ret.append(json);
      ret.append("}");
    } else {
      ret.append("curl -XPOST -H\"Content-Type: application/json\" -d @");
      ret.append(json);
      if (type.equals(TYPE.QUERY)) {
        ret.append(" http://127.0.0.1:6666/api/v1/datapoints/query");
      } else if (type.equals(TYPE.INSERT)) {
        ret.append(" http://127.0.0.1:6666/api/v1/datapoints");
      } else if (type.equals(TYPE.DELETE)) {
        ret.append(" http://127.0.0.1:6666/api/v1/datapoints/delete");
      }
    }
    return ret.toString();
  }

  public String execute(String json, TYPE type) throws Exception {
    StringBuilder ret = new StringBuilder();
    String curlArray = orderGen(json, type);
    Process process = null;
    try {
      ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
      processBuilder.directory(new File("./src/test/resources/restIT"));
      // 执行 url 命令
      process = processBuilder.start();

      // 输出子进程信息
      InputStreamReader inputStreamReaderINFO = new InputStreamReader(process.getInputStream());
      BufferedReader bufferedReaderINFO = new BufferedReader(inputStreamReaderINFO);
      String lineStr;
      while ((lineStr = bufferedReaderINFO.readLine()) != null) {
        ret.append(lineStr);
      }
      // 等待子进程结束
      process.waitFor();

      return ret.toString();
    } catch (InterruptedException e) {
      // 强制关闭子进程（如果打开程序，需要额外关闭）
      process.destroyForcibly();
      return null;
    }
  }

  private void executeAndCompare(String json, String output) {
    String result = "";
    try {
      result = execute(json, TYPE.QUERY);
    } catch (Exception e) {
      //            if (e.toString().equals())
      LOGGER.error("executeAndCompare fail. Caused by: {}.", e.toString());
    }
    assertEquals(output, result);
  }

  @Test
  public void testQueryWithoutTags() {
    String json = "testQueryWithoutTags.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]},{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.search\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"host\": [\"server2\"]}, \"values\": [[1359786400000,321.0]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWithTags() {
    String json = "testQueryWithTags.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWrongTags() {
    String json = "testQueryWrongTags.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryOneTagWrong() {
    String json = "testQueryOneTagWrong.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWrongName() {
    String json = "testQueryWrongName.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_.a.b.c\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryWrongTime() {
    String json = "testQueryWrongTime.json";
    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    executeAndCompare(json, result);
  }

  //    @Test
  //    public void testQuery(){
  //        String json = "";
  //        String result = "";
  //        executeAndCompare(json,result);
  //    }

  @Test
  public void testQueryAvg() {
    String json = "testQueryAvg.json";
    String result =
        "{\"queries\":[{\"sample_size\": 9,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788298001,1359788298001],[1359788398001,1359788398001],[1359788408001,1359788408001],[1359788298001,1359788300000],[1359788398001,1359788400000],[1359788408001,1359788410000],[1359788298001,13.2],[1359788398001,123.3],[1359788408001,23.1]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryCount() {
    String json = "testQueryCount.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,1359763200001],[1359763200001,1359849600000],[1359763200001,3]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryFirst() {
    String json = "testQueryFirst.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,1359763200001],[1359763200001,1359936000000],[1359763200001,13.2]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryLast() {
    String json = "testQueryLast.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,1359763200001],[1359763200001,1359936000000],[1359763200001,23.1]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryMax() {
    String json = "testQueryMax.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,1359763200001],[1359763200001,1359936000000],[1359763200001,123.3]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQueryMin() {
    String json = "testQueryMin.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,1359763200001],[1359763200001,1359936000000],[1359763200001,13.2]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testQuerySum() {
    String json = "testQuerySum.json";
    String result =
        "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"names\": [\"window_start\", \"window_end\", \"archive.file.tracked\"],\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,1359763200001],[1359763200001,1359936000000],[1359763200001,159.6]]}]}]}";
    executeAndCompare(json, result);
  }

  @Test
  public void testDelete() throws Exception {
    if (!isAbleToDelete) {
      return;
    }
    String json = "testDelete.json";
    execute(json, TYPE.DELETE);

    String result =
        "{\"queries\":[{\"sample_size\": 2,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"dc\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788410000,23.1]]}]}]}";
    json = "testQueryWithTags.json";
    executeAndCompare(json, result);
  }

  @Test
  public void testDeleteMetric() throws Exception {
    if (!isAbleToDelete) {
      return;
    }
    String json = "archive.file.tracked";
    execute(json, TYPE.DELETE_METRIC);

    String result =
        "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
    json = "testQueryWithTags.json";
    executeAndCompare(json, result);
  }

  @Test
  @Ignore
  // TODO this test makes no sense
  public void pathValidTest() {
    try {
      String res = execute("pathValidTest.json", TYPE.INSERT);
      LOGGER.warn("insertData fail. Caused by: {}.", res);
    } catch (Exception e) {
      LOGGER.error("insertData fail. Caused by: {}.", e.toString());
    }
  }
}
