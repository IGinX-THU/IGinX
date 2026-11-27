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
package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.metadata.utils.StorageEngineUtils.addressOf;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.physical.storage.descriptor.HostPolicy;
import org.junit.Test;

public class StorageEngineUtilsTest {

  @Test
  public void testResolveToIp() {
    assertEquals("127.0.0.1", addressOf("localhost", HostPolicy.RESOLVE_TO_IP));
    assertEquals("10.0.0.1", addressOf("10.0.0.1", HostPolicy.RESOLVE_TO_IP));
    // empty host is resolved to loopback
    assertEquals("127.0.0.1", addressOf("", HostPolicy.RESOLVE_TO_IP));
    assertNull(addressOf("no-such-host.invalid", HostPolicy.RESOLVE_TO_IP));
  }

  @Test
  public void testPreserveHost() {
    assertEquals(
        "oss-cn-hangzhou.aliyuncs.com",
        addressOf(" OSS-CN-HANGZHOU.aliyuncs.com ", HostPolicy.PRESERVE_HOST));
    assertEquals("localhost", addressOf("localhost", HostPolicy.PRESERVE_HOST));
    assertEquals("10.0.0.1", addressOf("10.0.0.1", HostPolicy.PRESERVE_HOST));
    // not resolved, so a host unknown to the local DNS is kept
    assertEquals(
        "no-such-host.invalid", addressOf("no-such-host.invalid", HostPolicy.PRESERVE_HOST));
    for (String invalid :
        new String[] {
          "", " ", "https://oss-cn-hangzhou.aliyuncs.com", "oss-cn-hangzhou.aliyuncs.com:443",
          "oss-cn-hangzhou.aliyuncs.com/bkt", "a..b", "-a.com", "a_b.com"
        }) {
      assertNull(invalid, addressOf(invalid, HostPolicy.PRESERVE_HOST));
    }
  }
}
