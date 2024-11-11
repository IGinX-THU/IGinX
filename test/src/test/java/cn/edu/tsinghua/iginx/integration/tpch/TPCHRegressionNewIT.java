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
package cn.edu.tsinghua.iginx.integration.tpch;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHRegressionNewIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TPCHRegressionNewIT.class);

  // host info
  protected static String defaultTestHost = "127.0.0.1";
  protected static int defaultTestPort = 6888;
  protected static String defaultTestUser = "root";
  protected static String defaultTestPass = "root";

  protected static Session session;

  static final String FAILED_QUERY_ID_PATH =
      "src/test/resources/tpch/runtimeInfo/failedQueryIds.txt";

  static final String ITERATION_TIMES_PATH =
      "src/test/resources/tpch/runtimeInfo/iterationTimes.txt";

  static final String MAIN_TIME_COSTS_PATH = "src/test/resources/tpch/runtimeInfo/oldTimeCosts.txt";

  static final String NEW_TIME_COSTS_PATH = "src/test/resources/tpch/runtimeInfo/newTimeCosts.txt";

  static final String STATUS_PATH = "src/test/resources/tpch/runtimeInfo/status.txt";

  // 最大重复测试次数
  int MAX_REPETITIONS_NUM;

  // 回归阈值
  double REGRESSION_THRESHOLD;

  List<Integer> queryIds;

  // 当前查询次数
  int iterationTimes;

  // 是否需要验证正确性
  boolean needValidate;

  public TPCHRegressionNewIT() {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    List<String> lines = TPCHUtils.getLinesFromFile(ITERATION_TIMES_PATH);
    iterationTimes = Integer.parseInt(lines.get(0));
    if (iterationTimes == 1) {
      queryIds = conf.getQueryIds();
    } else {
      lines = TPCHUtils.getLinesFromFile(FAILED_QUERY_ID_PATH);
      queryIds = new ArrayList<>();
      for (String line : lines) {
        queryIds.add(Integer.parseInt(line));
      }
    }
    // 第一次查询需要验证查询结果正确性
    needValidate = iterationTimes == 1;
    MAX_REPETITIONS_NUM = conf.getMaxRepetitionsNum();
    REGRESSION_THRESHOLD = conf.getRegressionThreshold();
  }

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  @Test
  public void testNewBranch() {
    if (queryIds.isEmpty()) {
      LOGGER.info("No query remain, skip test new branch.");
      return;
    }
    LOGGER.info("QueryIds remain: {}", queryIds);
    if (iterationTimes > MAX_REPETITIONS_NUM) {
      LOGGER.error(
          "Repeatedly executed query more than {} times, test failed.", MAX_REPETITIONS_NUM);
      Assert.fail();
    }

    List<Integer> failedQueryIds = new ArrayList<>();
    List<List<Long>> oldTimeCosts = TPCHUtils.readTimeCostsFromFile(MAIN_TIME_COSTS_PATH);
    List<List<Long>> newTimeCosts = TPCHUtils.readTimeCostsFromFile(NEW_TIME_COSTS_PATH);
    double ratio = 1 + REGRESSION_THRESHOLD;
    for (int queryId : queryIds) {
      long timeCost = TPCHUtils.executeTPCHQuery(session, queryId, needValidate);
      newTimeCosts.get(queryId - 1).add(timeCost);
      LOGGER.info(
          "Successfully execute TPC-H query {} in new branch in iteration {}, time cost: {}ms",
          queryId,
          iterationTimes,
          timeCost);

      // 新旧分支查询次数不相同
      if (oldTimeCosts.get(queryId - 1).size() != newTimeCosts.get(queryId - 1).size()) {
        LOGGER.error(
            "Query {} run {} times in main branch, but {} times in new branch, please check.",
            queryId,
            oldTimeCosts.get(queryId - 1).size(),
            newTimeCosts.get(queryId - 1).size());
        Assert.fail();
      }

      // 与主分支运行结果进行比较
      long oldTimeCostMedian = getMedian(oldTimeCosts.get(queryId - 1));
      long newTimeCostMedian = getMedian(newTimeCosts.get(queryId - 1));
      if (oldTimeCostMedian * ratio < newTimeCostMedian) {
        if (iterationTimes >= MAX_REPETITIONS_NUM) {
          LOGGER.error(
              "Repeatedly executed query {} more than {} times, old time costs' median: {}ms, new time costs' median: {}ms, test failed.",
              queryId,
              MAX_REPETITIONS_NUM,
              oldTimeCostMedian,
              newTimeCostMedian);
          Assert.fail();
        }
        failedQueryIds.add(queryId);
        continue;
      }
      LOGGER.info(
          "Query {} passed after {} times' iterations, old time costs' median: {}ms, new time costs' median: {}ms.",
          queryId,
          iterationTimes,
          oldTimeCostMedian,
          newTimeCostMedian);
    }

    if (failedQueryIds.isEmpty()) {
      writeOKtoFile();
    }

    TPCHUtils.clearAndRewriteTimeCostsToFile(newTimeCosts, NEW_TIME_COSTS_PATH);
    clearAndRewriteFailedQueryIdsToFile(failedQueryIds);
    updateIterationTimes();
  }

  private long getMedian(List<Long> array) {
    Collections.sort(array);
    int middle = array.size() / 2;
    return array.size() % 2 == 0
        ? (long) ((array.get(middle - 1) + array.get(middle)) / 2.0)
        : array.get(middle);
  }

  private void writeOKtoFile() {
    try (FileWriter fileWriter = new FileWriter(STATUS_PATH);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      // 清空文件内容
      fileWriter.write("");
      fileWriter.flush();
      // 重新写入内容
      bufferedWriter.write("ok");
    } catch (IOException e) {
      LOGGER.error("Write to file {} fail. Caused by:", STATUS_PATH, e);
      Assert.fail();
    }
  }

  private void clearAndRewriteFailedQueryIdsToFile(List<Integer> failedQueryIds) {
    Path path = Paths.get(FAILED_QUERY_ID_PATH);
    if (!Files.exists(path)) {
      try {
        Files.createFile(path);
      } catch (IOException e) {
        LOGGER.error("Failed to create file {}. Caused by: ", FAILED_QUERY_ID_PATH, e);
        Assert.fail();
      }
    }
    try (FileWriter fileWriter = new FileWriter(String.valueOf(path));
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      // 清空文件内容
      fileWriter.write("");
      fileWriter.flush();
      // 重新写入内容
      for (int failedQueryId : failedQueryIds) {
        bufferedWriter.write(String.valueOf(failedQueryId));
        bufferedWriter.newLine();
      }
    } catch (IOException e) {
      LOGGER.error("Write to file {} fail. Caused by:", path.getFileName(), e);
      Assert.fail();
    }
  }

  private void updateIterationTimes() {
    iterationTimes++;
    try (FileWriter fileWriter = new FileWriter(ITERATION_TIMES_PATH);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      // 清空文件内容
      fileWriter.write("");
      fileWriter.flush();
      // 重新写入内容
      bufferedWriter.write(String.valueOf(iterationTimes));
    } catch (IOException e) {
      LOGGER.error("Write to file {} fail. Caused by:", ITERATION_TIMES_PATH, e);
      Assert.fail();
    }
  }
}
