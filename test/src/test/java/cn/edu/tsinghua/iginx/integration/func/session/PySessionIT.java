package cn.edu.tsinghua.iginx.integration.func.session;


import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType.*;
import static org.junit.Assert.assertEquals;

public class PySessionIT {

  private static final Logger logger = LoggerFactory.getLogger(PySessionIT.class);

  // parameters to be flexibly configured by inheritance
  protected static MultiConnection session;
  private static boolean dummyNoData = true;
  private String pythonCMD = "/opt/homebrew/anaconda3/envs/iginx/bin/python";

  // host info
  protected String defaultTestHost = "127.0.0.1";
  protected int defaultTestPort = 6888;
  protected String defaultTestUser = "root";
  protected String defaultTestPass = "root";
  private static final TestDataSection baseDataSection = buildBaseDataSection();

  protected boolean isAbleToDelete;

  public PySessionIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
  }


  private static TestDataSection buildBaseDataSection() {
    List<String> paths =
            Arrays.asList(
                    "a.a.a", "a.a.b", "a.b.b", "a.c.c");
    List<DataType> types =
            Arrays.asList(
                    DataType.BINARY,
                    DataType.BINARY,
                    DataType.BINARY,
                    DataType.BINARY);
    List<Map<String, String>> tagsList =
            Arrays.asList(
                    new HashMap<>(),
                    new HashMap<>(),
                    new HashMap<>(),
                    new HashMap<>());
    // keys = [0, 1, 2, 3]
    List<Long> keys = IntStream.range(0, 4).mapToObj(i -> (long) i).collect(Collectors.toList());
    /*
     values = [
        ['a', 'b', None, None],
        [None, None, 'b', None],
        [None, None, None, 'c'],
        ['Q', 'W', 'E', 'R'],
    ]
    * */
    List<List<Object>> values =
            Arrays.asList(
                    Arrays.asList("a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8), null, null),
                    Arrays.asList(null, null, "b".getBytes(StandardCharsets.UTF_8), null),
                    Arrays.asList(null, null, null, "c".getBytes(StandardCharsets.UTF_8)),
                    Arrays.asList("Q".getBytes(StandardCharsets.UTF_8), "W".getBytes(StandardCharsets.UTF_8), "E".getBytes(StandardCharsets.UTF_8), "R".getBytes(StandardCharsets.UTF_8)));
    return new TestDataSection(keys, types, paths, values, tagsList);
  }
  private void insertData(TestDataSection data, InsertAPIType type) {
    switch (type) {
      case Row:
      case NonAlignedRow:
        Controller.writeRowsData(
                session,
                data.getPaths(),
                data.getKeys(),
                data.getTypes(),
                data.getValues(),
                data.getTagsList(),
                type,
                dummyNoData);
        break;
      case Column:
      case NonAlignedColumn:
        List<List<Object>> values =
                IntStream.range(0, data.getPaths().size())
                        .mapToObj(
                                col ->
                                        IntStream.range(0, data.getValues().size())
                                                .mapToObj(row -> data.getValues().get(row).get(col))
                                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
        Controller.writeColumnsData(
                session,
                data.getPaths(),
                IntStream.range(0, data.getPaths().size())
                        .mapToObj(i -> new ArrayList<>(data.getKeys()))
                        .collect(Collectors.toList()),
                data.getTypes(),
                values,
                data.getTagsList(),
                type,
                dummyNoData);
    }
  }
  @Before
  public void setUp() {
    try {
      session =
              new MultiConnection(
                      new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
      session.openSession();
      // insert base data using all types of insert API.
      List<InsertAPIType> insertAPITypes =
              Arrays.asList(Row, NonAlignedRow, Column, InsertAPIType.NonAlignedColumn);
      long start = 0, end = 4;
      TestDataSection subBaseData = baseDataSection.getSubDataSectionWithKey(start, end);
      insertData(subBaseData, insertAPITypes.get(0));
      dummyNoData = false;
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  @Test
    public void testQuery() {
      List <String> result = new ArrayList<>();
      try {
        // 设置Python脚本路径
        String pythonScriptPath = "../session_py/downsample.py";

        // 创建ProcessBuilder以执行Python脚本
        ProcessBuilder pb = new ProcessBuilder(pythonCMD, pythonScriptPath);

        // 启动进程并等待其终止
        Process process = pb.start();
        process.waitFor();

        // 读取Python脚本的输出
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
          System.out.println(line);
          result.add(line);
        }
        // 检查Python脚本是否正常终止
        int exitCode = process.exitValue();
        if (exitCode != 0) {
          System.err.println("Python script terminated with non-zero exit code: " + exitCode);
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
      // 检查Python脚本的输出是否符合预期
      List<String> expected = Arrays.asList(
              "Time\tcount(a.a.a)\tcount(a.a.b)\tcount(a.b.b)\tcount(a.c.c)\t",
              "0\t1\t1\t1\t1\t",
              "3\t1\t1\t1\t1\t",
              ""
      );
      // result只保留最后四行
      if(result.size() > 4) {
        result = result.subList(result.size() - 4, result.size());
      }
      assertEquals(result, expected);
    }
  @After
  public void tearDown() {
    try {
      clearData();
      session.closeSession();
    } catch (SessionException e) {
      logger.error(e.getMessage());
    }
  }
  protected void clearData() throws SessionException {
    if (session.isClosed()) {
      session =
              new MultiConnection(
                      new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
      session.openSession();
    }

    Controller.clearData(session);
  }
}
