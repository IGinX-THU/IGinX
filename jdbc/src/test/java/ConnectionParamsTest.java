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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.jdbc.Config;
import cn.edu.tsinghua.iginx.jdbc.IginXConnectionParams;
import cn.edu.tsinghua.iginx.jdbc.IginxUrlException;
import cn.edu.tsinghua.iginx.jdbc.Utils;
import java.util.Properties;
import org.junit.Test;

public class ConnectionParamsTest {

  @Test
  public void testParseURL() throws IginxUrlException {
    String userName = "root";
    String userPwd = "root";
    String host1 = "localhost";
    int port1 = 6888;
    Properties properties = new Properties();
    properties.setProperty(Config.USER, userName);

    properties.setProperty(Config.PASSWORD, userPwd);
    IginXConnectionParams params =
        Utils.parseUrl(String.format(Config.IGINX_URL_PREFIX + "%s:%s/", host1, port1), properties);
    assertEquals(host1, params.getHost());
    assertEquals(port1, params.getPort());
    assertEquals(userName, params.getUsername());
    assertEquals(userPwd, params.getPassword());

    String host2 = "127.0.0.1";
    int port2 = 6999;
    params =
        Utils.parseUrl(String.format(Config.IGINX_URL_PREFIX + "%s:%s", host2, port2), properties);
    assertEquals(params.getHost(), host2);
    assertEquals(params.getPort(), port2);
    assertEquals(params.getUsername(), userName);
    assertEquals(params.getPassword(), userPwd);
  }
}
