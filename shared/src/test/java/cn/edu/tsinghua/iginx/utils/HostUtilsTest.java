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
package cn.edu.tsinghua.iginx.utils;

import java.io.IOException;
import java.net.InetAddress;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HostUtilsTest {

  @Test
  public void testGetRepresentativeIP() throws IOException {
    String ip = HostUtils.getRepresentativeIP();
    // 这里使用System.out而不是用logger是因为logger在测试时并没有输出内容
    System.out.println(ip);

    InetAddress address = InetAddress.getByName(ip);
    if (address.isReachable(1000)) {
      System.out.println("successfully pinged.");
    } else {
      System.out.println("ping failed.");
      Assertions.fail();
    }
  }
}
