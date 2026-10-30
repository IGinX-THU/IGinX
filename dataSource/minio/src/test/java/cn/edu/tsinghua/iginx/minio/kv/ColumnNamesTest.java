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
package cn.edu.tsinghua.iginx.minio.kv;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public class ColumnNamesTest {

  @Test
  public void testRoundTripWithTags() {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("t1", "v,1");
    tags.put("t=2", "中文");
    String fullName = ColumnNames.toFullName("a.b/c.d e", tags);
    String encoded = ColumnNames.encode(fullName);
    assertFalse(encoded.contains("/"));
    ColumnKey key = ColumnNames.fromFullName(ColumnNames.decode(encoded));
    assertEquals("a.b/c.d e", key.getPath());
    assertEquals(tags, key.getTags());
  }

  @Test
  public void testLiteralPrefixMatchesEncodedNames() {
    String encoded =
        ColumnNames.encode(ColumnNames.toFullName("us.d1,x.s1", Collections.emptyMap()));
    assertTrue(encoded.startsWith(ColumnNames.encodedLiteralPrefix("us.d1,x.*")));
    assertTrue(encoded.startsWith(ColumnNames.encodedLiteralPrefix("us.d1,x.s1")));
    assertTrue(encoded.startsWith(ColumnNames.encodedLiteralPrefix("*")));
    assertFalse(encoded.startsWith(ColumnNames.encodedLiteralPrefix("us.d2.*")));
  }
}
