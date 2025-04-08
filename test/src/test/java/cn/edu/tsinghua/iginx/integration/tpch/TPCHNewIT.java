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
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHNewIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TPCHNewIT.class);

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

  List<String> queryIds;

  // 当前查询次数
  int iterationTimes;

  // 是否需要验证正确性
  boolean needValidate;

  public TPCHNewIT() throws IOException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    iterationTimes = TPCHUtils.getIterationTimesFromFile();
    queryIds = new ConfLoader(Controller.CONFIG_FILE).getQueryIds();
    // 第一次查询需要验证查询结果正确性
    needValidate = iterationTimes == 1;
    MAX_REPETITIONS_NUM = conf.getMaxRepetitionsNum();
  }

  @Test
  public void test() throws IOException {
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

    ArrayListMultimap<String, Long> timeCosts =
        TPCHUtils.readTimeCostsFromFile(TPCHUtils.NEW_TIME_COSTS_PATH);
    for (String queryId : queryIds) {
      long timeCost = TPCHUtils.executeTPCHQuery(session, queryId, needValidate);
      timeCosts.get(queryId).add(timeCost);
      System.out.printf(
          "Successfully execute TPC-H query %s in new branch in iteration %d, time cost: %dms%n",
          queryId, iterationTimes, timeCost);
    }
    TPCHUtils.clearAndRewriteTimeCostsToFile(timeCosts, TPCHUtils.NEW_TIME_COSTS_PATH);
  }
}
