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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
一、annotation测试逻辑：
1、正确操作测试，验证单一操作正确性
2、错误操作测试，验证错误操作，或者无效操作结果
3、重复性操作测试，测试可重复操作的结果是否正确
4、操作对象重复，测试操作逻辑中，可重复添加的元素是否符合逻辑
5、复杂操作，测试多种操作组合结果是否正确

二、annotation测试条目：
1、查询annotation信息
2、查询数据以及annotation信息
3、对每个修改操作单独测试，并通过两种查询分别验证正确性：
    3.1、测试 add（增加标签操作），通过queryAnno以及queryAll两种方法测试
    3.2、测试 update（更新标签操作），通过queryAnno以及queryAll两种方法测试
    3.3、测试 delete（删除标签操作），通过queryAnno以及queryAll两种方法测试
4、测试重复性操作操作，查看结果正确性
    4.1、测试添加相同category，通过queryAnno以及queryAll两种方法测试
    4.2、测试不断更新相同结果的category，通过queryAnno以及queryAll两种方法测试
    4.3、测试不断删除的category，通过queryAnno以及queryAll两种方法测试
5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性

 */
public class RestAnnotationIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestAnnotationIT.class);

  private static Session session;

  protected boolean isAbleToDelete;

  public RestAnnotationIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
  }

  public enum TYPE {
    APPEND,
    UPDATE,
    INSERT,
    QUERY_ANNOTATION,
    QUERY_ALL,
    DELETE
  }

  private static final String[] API = {
    " http://127.0.0.1:6666/api/v1/datapoints/annotations/add",
    " http://127.0.0.1:6666/api/v1/datapoints/annotations/update",
    " http://127.0.0.1:6666/api/v1/datapoints/annotations",
    " http://127.0.0.1:6666/api/v1/datapoints/query/annotations",
    " http://127.0.0.1:6666/api/v1/datapoints/query/annotations/data",
    " http://127.0.0.1:6666/api/v1/datapoints/annotations/delete",
  };

  private static final String PREFIX = "curl -XPOST -H\"Content-Type: application/json\" -d @";

  private static final DataType[] DATA_TYPE_ARRAY =
      new DataType[] {DataType.LONG, DataType.DOUBLE, DataType.BINARY};

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  private String orderGen(String fileName, TYPE type) {
    return PREFIX + fileName + API[type.ordinal()];
  }

  private String execute(String fileName, TYPE type, DataType dataType) throws Exception {
    StringBuilder ret = new StringBuilder();
    String curlArray = orderGen(fileName, type);
    Process process = null;
    try {
      ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
      String dir;
      if (type.equals(TYPE.INSERT)) {
        dir =
            String.format(
                "./src/test/resources/restAnnotation/%sType", dataType.toString().toLowerCase());
      } else {
        dir = "./src/test/resources/restAnnotation/common";
      }
      processBuilder.directory(new File(dir));

      // 执行 url 命令
      process = processBuilder.start();

      // 输出子进程信息
      InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream());
      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
      String lineStr;
      while ((lineStr = bufferedReader.readLine()) != null) {
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

  private void insertData(DataType dataType) {
    try {
      execute("insert.json", TYPE.INSERT, dataType);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  private void executeAndCompare(String json, String expected, TYPE type, DataType dataType) {
    try {
      List<String> expectedResult =
          JSON.parseArray(JSONObject.parseObject(expected).getString("queries"), String.class);
      List<String> actualResult =
          JSON.parseArray(
              JSONObject.parseObject(execute(json, type, dataType)).getString("queries"),
              String.class);
      assertEquals(new HashSet<>(expectedResult), new HashSet<>(actualResult));
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  private String getAns(String fileName, DataType dataType) {
    StringBuilder ret = new StringBuilder();
    switch (dataType) {
      case DOUBLE:
      case LONG:
      case BINARY:
        if (fileName.endsWith("Anno")) {
          fileName =
              String.format("./src/test/resources/restAnnotation/common/ans/%s.json", fileName);
        } else {
          fileName =
              String.format(
                  "./src/test/resources/restAnnotation/%sType/ans/%s.json",
                  dataType.toString().toLowerCase(), fileName);
        }
        break;
      default:
        throw new IllegalStateException("Unexpected DataType: " + dataType);
    }

    File file = new File(fileName);
    try {
      FileReader fr = new FileReader(file);
      try (BufferedReader br = new BufferedReader(fr)) {
        String line;
        while ((line = br.readLine()) != null) {
          ret.append(line);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
    return removeSpecialChar(ret.toString());
  }

  // 去除字符串中的空格、回车、换行符、制表符等
  private String removeSpecialChar(String str) {
    String s = "";
    if (str != null) {
      // 定义含特殊字符的正则表达式
      Pattern p = Pattern.compile("\\s*|\t|\r|\n");
      Matcher m = p.matcher(str);
      s = m.replaceAll("");
    }
    return s;
  }

  private String getMethodName() {
    StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
    StackTraceElement e = stacktrace[2];
    return e.getMethodName();
  }

  private void clearData() {
    try {
      Controller.clearData(session);
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  @Test
  public void testAll() {
    for (DataType dataType : DATA_TYPE_ARRAY) {
      LOGGER.info("Testing datatype: {}...", dataType);
      testQueryAnno(dataType);
      testQueryAll(dataType);

      testAppendViaQueryAnno(dataType);
      testAppendViaQueryAll(dataType);

      testDuplicateAppend2ViaQueryAll(dataType);
      testDuplicateAppendViaQueryAll(dataType);

      testSameAppendViaQueryAll(dataType);

      if (isAbleToDelete) {
        testUpdateViaQueryAnno(dataType);
        testUpdateViaQueryAll(dataType);
        testDeleteViaQueryAnno(dataType);
        testDeleteViaQueryAll(dataType);

        testDuplicateUpdateViaQueryAnno(dataType);
        testDuplicateUpdateViaQueryAll(dataType);
        testDuplicateDeleteViaQueryAnno(dataType);
        testDuplicateDeleteViaQueryAll(dataType);

        testSameUpdateViaQueryAll(dataType);

        testAppend2ViaQueryAll(dataType);
      }
    }
  }

  // 查询annotation信息
  private void testQueryAnno(DataType dataType) {
    insertData(dataType);
    String ans = getAns(getMethodName(), dataType);
    executeAndCompare("queryAnno.json", ans, TYPE.QUERY_ANNOTATION, dataType);
    clearData();
  }

  // 查询数据以及annotation信息
  private void testQueryAll(DataType dataType) {
    insertData(dataType);
    String ans = getAns(getMethodName(), dataType);
    executeAndCompare("queryData.json", ans, TYPE.QUERY_ALL, dataType);
    clearData();
  }

  // 执行添加annotation操作，并查询annotation信息
  private void testAppendViaQueryAnno(DataType dataType) {
    insertData(dataType);
    try {
      execute("add.json", TYPE.APPEND, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("appendViaQueryAnno.json", ans, TYPE.QUERY_ANNOTATION, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 执行添加annotation操作，并查询数据以及annotation信息
  private void testAppendViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("add.json", TYPE.APPEND, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("appendViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 执行更新annotation操作，并查询annotation信息
  private void testUpdateViaQueryAnno(DataType dataType) {
    insertData(dataType);
    try {
      execute("update.json", TYPE.UPDATE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("updateViaQueryAnno.json", ans, TYPE.QUERY_ANNOTATION, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 执行更新annotation操作，并查询数据以及annotation信息
  private void testUpdateViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("update.json", TYPE.UPDATE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("updateViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 执行删除annotation操作，并查询annotation信息
  private void testDeleteViaQueryAnno(DataType dataType) {
    insertData(dataType);
    try {
      execute("delete.json", TYPE.DELETE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("deleteViaQueryAnno.json", ans, TYPE.QUERY_ANNOTATION, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 执行删除annotation操作，并查询数据以及annotation信息
  private void testDeleteViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("delete.json", TYPE.DELETE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("deleteViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行添加annotation操作，并查询数据以及annotation信息
  private void testDuplicateAppend2ViaQueryAll(DataType dataType) {
    try {
      execute("insert2.json", TYPE.INSERT, dataType);
      execute("add2.json", TYPE.APPEND, dataType);
      execute("add2.json", TYPE.APPEND, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("append2ViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行添加annotation操作，并查询数据以及annotation信息
  private void testDuplicateAppendViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("add.json", TYPE.APPEND, dataType);
      execute("add.json", TYPE.APPEND, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("appendViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行更新annotation操作，并查询annotation信息
  private void testDuplicateUpdateViaQueryAnno(DataType dataType) {
    insertData(dataType);
    try {
      execute("update.json", TYPE.UPDATE, dataType);
      execute("update.json", TYPE.UPDATE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("updateViaQueryAnno.json", ans, TYPE.QUERY_ANNOTATION, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行更新annotation操作，并查询数据以及annotation信息
  private void testDuplicateUpdateViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("update.json", TYPE.UPDATE, dataType);
      execute("update.json", TYPE.UPDATE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("updateViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行删除annotation操作，并查询annotation信息
  private void testDuplicateDeleteViaQueryAnno(DataType dataType) {
    insertData(dataType);
    try {
      execute("delete.json", TYPE.DELETE, dataType);
      execute("delete.json", TYPE.DELETE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("deleteViaQueryAnno.json", ans, TYPE.QUERY_ANNOTATION, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行删除annotation操作，并查询数据以及annotation信息
  private void testDuplicateDeleteViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("delete.json", TYPE.DELETE, dataType);
      execute("delete.json", TYPE.DELETE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("deleteViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行添加annotation操作，并查询数据以及annotation信息
  private void testSameAppendViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("addSame.json", TYPE.APPEND, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("appendViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 重复执行更新annotation操作，并查询数据以及annotation信息
  private void testSameUpdateViaQueryAll(DataType dataType) {
    insertData(dataType);
    try {
      execute("updateSame.json", TYPE.UPDATE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("queryData.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }

  // 依次执行添加和删除annotation操作，并查询数据以及annotation信息
  private void testAppend2ViaQueryAll(DataType dataType) {
    try {
      execute("insert2.json", TYPE.INSERT, dataType);
      execute("add2.json", TYPE.APPEND, dataType);
      execute("delete.json", TYPE.DELETE, dataType);
      String ans = getAns(getMethodName(), dataType);
      executeAndCompare("append2ViaQueryAll.json", ans, TYPE.QUERY_ALL, dataType);
      clearData();
    } catch (Exception e) {
      LOGGER.error("Error occurred during execution ", e);
      fail();
    }
  }
}
