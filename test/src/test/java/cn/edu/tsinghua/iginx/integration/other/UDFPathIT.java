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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.integration.other;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.session.Session;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFPathIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFPathIT.class);

  private static Session session;

  // host info
  private static final String defaultTestHost = "127.0.0.1";
  private static final int defaultTestPort = 6888;
  private static final String defaultTestUser = "root";
  private static final String defaultTestPass = "root";

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

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
  public void testUDFFuncList() {
    String statement = "SHOW FUNCTIONS;";
    String expectedRes = SQLTestTools.execute(session, statement);
    assert expectedRes.contains("Functions info:");
    List<String> udfList = config.getUdfList();
    // result should contain all udfs registered in config file
    for (String udf : udfList) {
      String[] udfInfo = udf.split(",");
      assert expectedRes.contains(udfInfo[1]);
    }
  }
}
