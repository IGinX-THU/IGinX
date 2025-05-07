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
package cn.edu.tsinghua.iginx.integration.distributed.startup;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.io.IOException;
import java.text.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterIT {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterIT.class);

  protected static String SHOW_CLUSTER_INFO = "SHOW CLUSTER INFO;";

  protected final Session session6888 = new Session("127.0.0.1", 6888);

  protected final Session session6889 = new Session("127.0.0.1", 6889);

  protected final Session session6890 = new Session("127.0.0.1", 6890);

  @Before
  public void setUp() throws SessionException, IOException, ParseException {
    session6888.openSession();
    session6889.openSession();
    session6890.openSession();
  }

  @After
  public void tearDown() throws SessionException {
    session6888.closeSession();
    session6889.closeSession();
    session6890.closeSession();
  }

  @Test
  public void testStartUp() {
    testShowClusterInfo(session6888);
    testShowClusterInfo(session6889);
    testShowClusterInfo(session6890);
  }

  private void testShowClusterInfo(Session session) {
    try {
      LOGGER.info(
          "Execute SHOW CLUSTER INFO for session (host: {}, port: {})",
          session.getHost(),
          session.getPort());
      SessionExecuteSqlResult res = session.executeSql(SHOW_CLUSTER_INFO);
      String result = res.getResultInString(false, "");
      LOGGER.info("Result: \"{}\"", result);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"SHOW CLUSTER INFO;\" execute fail. Caused by: ", e);
      fail();
    }
  }
}
