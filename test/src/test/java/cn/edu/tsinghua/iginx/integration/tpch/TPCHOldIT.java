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
import com.google.common.collect.ArrayListMultimap;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHOldIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TPCHOldIT.class);

  protected final Session session = new Session("127.0.0.1", 6888);

  @Before
  public void setUp() throws SessionException, IOException, ParseException {
    session.openSession();
    session.executeSql("CLEAR DATA;");
    TPCHUtils.insert(session);
  }

  @After
  public void tearDown() throws SessionException {
    session.executeSql("CLEAR DATA;");
    session.closeSession();
  }

  // 最大重复测试次数
  int MAX_REPETITIONS_NUM;

  // 回归阈值
  double REGRESSION_THRESHOLD;

  List<String> queryIds;

  List<String> allQueryIds;

  // 当前查询次数
  int iterationTimes;

  public TPCHOldIT() throws IOException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    iterationTimes = TPCHUtils.getIterationTimesFromFile();
    queryIds = TPCHUtils.getFailedQueryIdsFromFile();
    allQueryIds = new ConfLoader(Controller.CONFIG_FILE).getQueryIds();
    MAX_REPETITIONS_NUM = conf.getMaxRepetitionsNum();
    REGRESSION_THRESHOLD = conf.getRegressionThreshold();
  }

  @Test
  public void test() throws IOException {
    if (queryIds.isEmpty()) {
      LOGGER.info("No query remain, skip test main branch.");
      return;
    }
    LOGGER.info("QueryIds remain: {}", queryIds);
    if (iterationTimes > MAX_REPETITIONS_NUM) {
      LOGGER.error(
          "Repeatedly executed query more than {} times, test failed.", MAX_REPETITIONS_NUM);
      Assert.fail();
    }

    TPCHUtils.showRules(session);

    List<String> failedQueryIds = new ArrayList<>();
    ArrayListMultimap<String, Long> oldTimeCosts =
        TPCHUtils.readTimeCostsFromFile(TPCHUtils.OLD_TIME_COSTS_PATH);
    ArrayListMultimap<String, Long> newTimeCosts =
        TPCHUtils.readTimeCostsFromFile(TPCHUtils.NEW_TIME_COSTS_PATH);
    double ratio = 1 + REGRESSION_THRESHOLD;
    for (String queryId : allQueryIds) {
      long timeCost = TPCHUtils.executeTPCHQuery(session, queryId, false);
      oldTimeCosts.get(queryId).add(timeCost);
      LOGGER.info(
          "Successfully execute TPC-H query {} in old branch in iteration {}, time cost: {}ms",
          queryId,
          iterationTimes,
          timeCost);
    }

    for (String queryId : queryIds) {
      // 新旧分支查询次数不相同
      if (oldTimeCosts.get(queryId).size() != newTimeCosts.get(queryId).size()) {
        LOGGER.error(
            "Query {} run {} times in old branch, but {} times in new branch, please check.",
            queryId,
            oldTimeCosts.get(queryId).size(),
            newTimeCosts.get(queryId).size());
        Assert.fail();
      }

      // 与主分支运行结果进行比较
      long oldTimeCostMedian = getMedian(oldTimeCosts.get(queryId));
      long newTimeCostMedian = getMedian(newTimeCosts.get(queryId));
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
        LOGGER.info(
            "Query {} failed after {} times' iterations, old time costs' median: {}ms, new time costs' median: {}ms.",
            queryId,
            iterationTimes,
            oldTimeCostMedian,
            newTimeCostMedian);
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

    TPCHUtils.clearAndRewriteTimeCostsToFile(oldTimeCosts, TPCHUtils.OLD_TIME_COSTS_PATH);
    TPCHUtils.clearAndRewriteFailedQueryIdsToFile(failedQueryIds);
    TPCHUtils.rewriteIterationTimes(iterationTimes + 1);
  }

  private long getMedian(List<Long> array) {
    Collections.sort(array);
    int middle = array.size() / 2;
    return array.size() % 2 == 0
        ? (long) ((array.get(middle - 1) + array.get(middle)) / 2.0)
        : array.get(middle);
  }

  private void writeOKtoFile() {
    try (FileWriter fileWriter = new FileWriter(TPCHUtils.STATUS_PATH);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      // 清空文件内容
      fileWriter.write("");
      fileWriter.flush();
      // 重新写入内容
      bufferedWriter.write("ok");
    } catch (IOException e) {
      LOGGER.error("Write to file {} fail. Caused by:", TPCHUtils.STATUS_PATH, e);
      Assert.fail();
    }
  }
}
