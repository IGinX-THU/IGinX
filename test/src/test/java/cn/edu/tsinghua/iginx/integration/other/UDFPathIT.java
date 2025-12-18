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

import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.func.udf.UDFTestTools;
import cn.edu.tsinghua.iginx.integration.tool.ClientLauncher;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFPathIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFPathIT.class);

  private static Session session;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  // host info
  private static String defaultTestHost = "127.0.0.1";
  private static int defaultTestPort = 6888;
  private static String defaultTestUser = "root";
  private static String defaultTestPass = "root";

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
  public void testUDFPath() throws IOException {
    testUDFFuncList();
    testRegisterUDFByClientUsingRelativePath();
  }

  /** ensure every udf in list is registered */
  private void testUDFFuncList() {
    List<String> UdfNameList;
    List<String> UdfList = config.getUdfList();
    UdfNameList =
        UdfList.stream()
            .map(
                udf -> {
                  String[] udfInfo = udf.split(",");
                  if (udfInfo.length != 4) {
                    LOGGER.error("udf info len must be 4.");
                  }
                  return udfInfo[1];
                })
            .collect(Collectors.toList());
    UDFTestTools tools = new UDFTestTools(session);
    tools.isUDFsRegistered(UdfNameList);
  }

  private void testRegisterUDFByClientUsingRelativePath() throws IOException {
    ClientLauncher client = new ClientLauncher();
    Path source = Paths.get("src", "test", "resources", "udf", "mock_udf.py");
    Path target = Paths.get(client.getClientRootDir(), "mock_udf.py");
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

    try {
      String statement = "CREATE FUNCTION UDAF \"mock_udf\" FROM \"MockUDF\" IN \"mock_udf.py\";";
      client.readLine(statement);
      assertTrue(client.expectedOutputContains("success"));

      statement = "DROP FUNCTION \"mock_udf\";";
      client.readLine(statement);
      assertTrue(client.expectedOutputContains("success"));
    } finally {
      client.close();
    }
  }
}
