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
package cn.edu.tsinghua.iginx.integration.func.auth;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserManagementIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserManagementIT.class);

  private Session rootSession;

  @BeforeEach
  public void setUp() throws SessionException {
    rootSession = new Session("127.0.0.1", 6888, "root", "root");
    rootSession.openSession();
  }

  @AfterEach
  public void tearDown() throws SessionException {
    rootSession.closeSession();
    rootSession = null;
  }

  @Test
  public void testUserGuideSample() throws SessionException {
    rootSession.executeSql("CREATE USER root1 IDENTIFIED BY root1;");
    rootSession.executeSql("GRANT WRITE, READ TO USER root1;");
    rootSession.executeSql("GRANT READ TO USER root1;");
    rootSession.executeSql("ALTER USER root1 IDENTIFIED BY root2;");

    {
      SessionExecuteSqlResult res = rootSession.executeSql("SHOW USER root, root1;");
      String resultInString = res.getResultInString(false, null);
      String expected =
          "User Info:\n"
              + "+-----+-------------+-----------------------------+\n"
              + "| name|         type|                        auths|\n"
              + "+-----+-------------+-----------------------------+\n"
              + "| root|Administrator|[Read, Write, Admin, Cluster]|\n"
              + "|root1| OrdinaryUser|                       [Read]|\n"
              + "+-----+-------------+-----------------------------+\n";
      assertEquals(expected, resultInString);
    }

    rootSession.executeSql("DROP USER root1;");

    {
      SessionExecuteSqlResult res = rootSession.executeSql("SHOW USER;");
      String resultInString = res.getResultInString(false, null);
      String expected =
          "User Info:\n"
              + "+----+-------------+-----------------------------+\n"
              + "|name|         type|                        auths|\n"
              + "+----+-------------+-----------------------------+\n"
              + "|root|Administrator|[Read, Write, Admin, Cluster]|\n"
              + "+----+-------------+-----------------------------+\n";
      assertEquals(expected, resultInString);
    }
  }
}
