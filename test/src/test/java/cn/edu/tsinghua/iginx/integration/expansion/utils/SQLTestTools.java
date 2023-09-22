package cn.edu.tsinghua.iginx.integration.expansion.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLTestTools {

  private static final Logger logger = LoggerFactory.getLogger(SQLTestTools.class);

  public static void executeAndCompare(Session session, String statement, String exceptOutput) {
    String actualOutput = execute(session, statement);
    assertEquals(exceptOutput, actualOutput);
  }

  private static String execute(Session session, String statement) {
    logger.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException | ExecutionException e) {
      logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }

  private static void compareValuesList(
      List<List<Object>> expectedValuesList, List<List<Object>> actualValuesList) {
    Set<List<String>> expectedSet =
        expectedValuesList.stream()
            .map(
                row -> {
                  List<String> strValues = new ArrayList<>();
                  row.forEach(
                      val -> {
                        if (val instanceof byte[]) {
                          strValues.add(new String((byte[]) val));
                        } else {
                          strValues.add(String.valueOf(val));
                        }
                      });
                  return strValues;
                })
            .collect(Collectors.toSet());

    Set<List<String>> actualSet =
        actualValuesList.stream()
            .map(
                row -> {
                  List<String> strValues = new ArrayList<>();
                  row.forEach(
                      val -> {
                        if (val instanceof byte[]) {
                          strValues.add(new String((byte[]) val));
                        } else {
                          strValues.add(String.valueOf(val));
                        }
                      });
                  return strValues;
                })
            .collect(Collectors.toSet());

    if (!expectedSet.equals(actualSet)) {
      logger.error("actual valuesList is {} and it should be {}", actualSet, expectedSet);
      fail();
    }
  }

  public static void executeAndCompare(
      Session session,
      String statement,
      List<String> pathListAns,
      List<List<Object>> expectedValuesList) {
    try {
      SessionExecuteSqlResult res = session.executeSql(statement);
      List<String> pathList = res.getPaths();
      List<List<Object>> actualValuesList = res.getValues();

      for (int i = 0; i < pathListAns.size(); i++) {
        assertEquals(pathListAns.get(i), pathList.get(i));
      }

      compareValuesList(expectedValuesList, actualValuesList);
    } catch (SessionException | ExecutionException e) {
      logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }
  }

    public static int executeShellScript(String scriptPath, String... args) {
        try {
            // 构建shell命令
            String[] command = new String[args.length + 2];
            command[0] = "sh";
            command[1] = scriptPath;
            System.arraycopy(args, 0, command, 2, args.length);

            // 创建进程并执行命令
            logger.info("exe shell : {}", Arrays.toString(command));
            ProcessBuilder processBuilder = new ProcessBuilder(command);

            // 设置工作目录（可选）
            processBuilder.directory(new File("../"));

            Process process = processBuilder.start();

            // 读取脚本输出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // 等待脚本执行完毕
            int exitCode = process.waitFor();
            System.out.println("脚本执行完毕，退出码：" + exitCode);
            return exitCode;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
