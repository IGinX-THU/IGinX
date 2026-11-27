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
package cn.edu.tsinghua.iginx.filesystem.server;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.filesystem.service.rpc.InternalAuth;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

public class InternalAuthTest {
  @Test
  public void bindsMethodArgumentsAndCanonicalMaps() throws Exception {
    String secret = "012345678901234567890123";
    RawPrefix prefix = new RawPrefix().setPrefix("indagent");
    String signature = InternalAuth.sign(secret, "getUnits", prefix);
    assertNotEquals(signature, InternalAuth.sign(secret, "delete", prefix));
    assertNotEquals(signature, InternalAuth.sign("other-secret-0123456789012", "getUnits", prefix));
    prefix.setCredential(signature);
    assertEquals(signature, InternalAuth.sign(secret, "getUnits", prefix));
    prefix.setPrefix("business");
    assertNotEquals(signature, InternalAuth.sign(secret, "getUnits", prefix));
    RawInserted data =
        new RawInserted()
            .setPatterns(Arrays.asList("a.x"))
            .setTagsList(Arrays.asList(new LinkedHashMap<>()))
            .setKeys(ByteBuffer.wrap(new byte[] {1}))
            .setValuesList(Arrays.asList(ByteBuffer.wrap(new byte[] {2})))
            .setBitmapList(Arrays.asList(ByteBuffer.wrap(new byte[] {1})))
            .setDataTypeList(Arrays.asList("LONG"))
            .setRawDataType("Row");
    data.getTagsList().get(0).put("z", "1");
    data.getTagsList().get(0).put("a", "2");
    DataUnit unit = new DataUnit(false).setName("unit");
    signature = InternalAuth.sign(secret, "insert", unit, data);
    RawInserted decoded = new RawInserted();
    new TDeserializer(new TBinaryProtocol.Factory())
        .deserialize(decoded, new TSerializer(new TBinaryProtocol.Factory()).serialize(data));
    assertEquals(signature, InternalAuth.sign(secret, "insert", unit, decoded));
    assertNotEquals(
        signature,
        InternalAuth.sign(secret, "insert", new DataUnit(false).setName("other"), decoded));
    decoded.setValuesList(Arrays.asList(ByteBuffer.wrap(new byte[] {3})));
    assertNotEquals(signature, InternalAuth.sign(secret, "insert", unit, decoded));
    assertNull(InternalAuth.sign(null, "getUnits", prefix));
  }
}
