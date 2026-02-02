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
package cn.edu.tsinghua.iginx.integration.other;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.JobState;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformJobRestartIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformJobRestartIT.class);

  private static Session session;

  // host info
  private static String defaultTestHost = "127.0.0.1";
  private static int defaultTestPort = 6888;
  private static String defaultTestUser = "root";
  private static String defaultTestPass = "root";

  private static final String COMMIT_SQL_FORMATTER = "COMMIT TRANSFORM JOB \"%s\";";

  private static final String OUTPUT_DIR_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "transform";

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
  public void prepare() throws SessionException {
    LOGGER.info("preparing scheduled job...");
    String yamlFileName =
        OUTPUT_DIR_PREFIX + File.separator + "TransformScheduledEvery10sNoExport.yaml";
    long jobId = session.commitTransformJob(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
    LOGGER.info("job id: {}", jobId);
  }

  @Test
  public void verifyJobExists() throws SessionException {
    Map<JobState, List<Long>> jobStateListMap = session.showEligibleJob(null);
    boolean found = false;
    long jobId = -1;
    if (jobStateListMap != null) {
      for (Map.Entry<JobState, List<Long>> entry : jobStateListMap.entrySet()) {
        if (entry.getKey() == JobState.JOB_IDLE || entry.getKey() == JobState.JOB_RUNNING) {
          // check idle & running jobs, there should be only one job
          if (!entry.getValue().isEmpty()) {
            found = true;
            jobId = entry.getValue().get(0);
          }
        }
      }
    }
    if (!found) {
      LOGGER.info("job not found.");
      fail();
    } else {
      LOGGER.info("job exists.");
      session.cancelTransformJob(jobId);
    }
  }
}
