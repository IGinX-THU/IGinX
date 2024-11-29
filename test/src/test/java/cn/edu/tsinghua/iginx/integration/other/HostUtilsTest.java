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

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.IginxInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HostUtilsTest.class);

  private static Session session;

  @BeforeClass
  public static void setUp() throws SessionException {
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    session.closeSession();
  }

  /** 配置文件中ip设置为0.0.0.0时，需要找到IGinX所在节点的真实ip。这里通过建立这个ip和端口的session来验证ip是否寻找正确 */
  @Test
  public void getRealIp() {
    try {
      IginxInfo info = session.getClusterInfo().getIginxInfos().get(0);
      String realIp = info.ip;
      int port = info.port;

      Session newSession = new Session(realIp, port, "root", "root");
      newSession.openSession();
      try {
        int iginxCount = newSession.getClusterInfo().getIginxInfos().size();
        assert iginxCount >= 1;
      } finally {
        newSession.closeSession();
      }

    } catch (SessionException e) {
      LOGGER.error("Error occurred in session:", e);
    }
  }
}
