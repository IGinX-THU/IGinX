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
package cn.edu.tsinghua.iginx.integration.expansion.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLTestTools {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLTestTools.class);

  public static void executeAndCompare(Session session, String statement, String exceptOutput) {
    String actualOutput = execute(session, statement);
    assertEquals(exceptOutput, actualOutput);
  }

  private static String execute(Session session, String statement) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }

  private static void compareValuesList(
      List<List<Object>> expectedValuesList, List<List<Object>> actualValuesList) {
    compareValuesList(expectedValuesList, actualValuesList, true);
  }

  private static void containValuesList(
      List<List<Object>> expectedValuesList, List<List<Object>> actualValuesList) {
    compareValuesList(expectedValuesList, actualValuesList, false);
  }

  private static void compareValuesList(
      List<List<Object>> expectedValuesList,
      List<List<Object>> actualValuesList,
      boolean equalMode) {
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

    if (equalMode && !expectedSet.equals(actualSet)) {
      LOGGER.error("actual valuesList is {} and it should be {}", actualSet, expectedSet);
      fail();
    } else if (!equalMode && !actualSet.containsAll(expectedSet)) {
      LOGGER.error("actual valuesList is {} and it should contain {}", actualSet, expectedSet);
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

      assertArrayEquals(new List[] {pathListAns}, new List[] {pathList});

      compareValuesList(expectedValuesList, actualValuesList);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }
  }

  /** execute query and result should contain expected values for specified paths. */
  public static void executeAndContainValue(
      Session session,
      String statement,
      List<String> pathListAns,
      List<List<Object>> expectedValuesList) {
    try {
      SessionExecuteSqlResult res = session.executeSql(statement);
      List<String> pathList = res.getPaths();
      List<List<Object>> actualValuesList = res.getValues();

      assertArrayEquals(new List[] {pathListAns}, new List[] {pathList});

      containValuesList(expectedValuesList, actualValuesList);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }
  }

  public static int executeShellScript(String scriptPath, String... args) {
    try {
      // 构建shell命令，action中的windows runner需要使用绝对路径
      String[] command;
      boolean isOnWin = System.getProperty("os.name").toLowerCase().contains("win");
      command = new String[args.length + 2];
      command[1] = scriptPath;
      System.arraycopy(args, 0, command, 2, args.length);
      if (isOnWin && !ShellRunner.isCommandOnPath("bash")) {
        command[0] = "C:/Program Files/Git/bin/bash.exe";
      } else {
        command[0] = "bash";
      }
      // 创建进程并执行命令
      LOGGER.info("exe shell : {}", Arrays.toString(command));
      ProcessBuilder processBuilder = new ProcessBuilder(command);

      // 设置工作目录（可选）
      processBuilder.directory(new File("../"));
      processBuilder.redirectErrorStream(true);
      Process process = processBuilder.start();

      if (isOnWin) {
        // on windowsOS, the bash script calls a batch script to start iginx.
        // If waits, java will wait for the iginx process to end, which would take forever.
        // thus create a new process to read the output.
        new Thread(
                () -> {
                  try (BufferedReader reader =
                      new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                      System.out.println(line);
                    }
                  } catch (IOException e) {
                    LOGGER.error("unexpected error: ", e);
                  }
                })
            .start();

        // sleep 45s for new thread to print script output(30s timeout in script)
        Thread.sleep(45000);

        return 0;
      } else {
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
      }
    } catch (IOException | InterruptedException e) {
      LOGGER.error("unexpected error: ", e);
    }
    return 0;
  }
}
