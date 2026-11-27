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
package cn.edu.tsinghua.iginx.auth;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.FileChunk;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import org.junit.Test;

public class UploadRegistryTest {
  @Test
  public void ownedUploadsAndTraversal() throws Exception {
    UploadRegistry registry = UploadRegistry.getInstance();
    for (String name : new String[] {"../config.properties", "a/b", "a\\b", "C:secret", "", "x..y"})
      assertFalse(UploadRegistry.validName(name));
    String name = "upload-" + System.nanoTime() + ".csv";
    FileChunk chunk =
        new FileChunk()
            .setFileName(name)
            .setOffset(0)
            .setData(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertEquals(RpcUtils.SUCCESS, registry.write(1, chunk));
    assertFalse(registry.owns(2, name));
    assertEquals(RpcUtils.ACCESS_DENY, registry.write(2, chunk));
    try {
      registry.path(2, name);
      fail("Foreign upload was readable");
    } catch (SecurityException expected) {
    }
    assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(registry.path(1, name)));
    registry.release(1, name);
    assertFalse(registry.owns(1, name));
  }
}
