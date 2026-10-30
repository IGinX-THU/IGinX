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

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.utils.Escaper;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps a column (path + tags) to one component of an object key and back.
 *
 * <p>The column is first translated to a full name {@code path,k1=v1,k2=v2} (same as redis), then
 * URL-encoded, so that it never contains {@code /}. Both steps encode char by char, hence the
 * encoded literal prefix of a pattern is a prefix of the encoded names it matches, which allows
 * listing objects by prefix.
 */
public final class ColumnNames {

  private static final ColumnKeyTranslator TRANSLATOR =
      new ColumnKeyTranslator(',', '=', getEscaper());

  private ColumnNames() {}

  private static Escaper getEscaper() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put(',', ',');
    replacementMap.put('=', '=');
    return new Escaper('\\', replacementMap);
  }

  public static String toFullName(String path, Map<String, String> tags) {
    return TRANSLATOR.translate(new ColumnKey(path, tags == null ? Collections.emptyMap() : tags));
  }

  public static ColumnKey fromFullName(String fullName) {
    try {
      return TRANSLATOR.translate(fullName);
    } catch (ParseException e) {
      throw new IllegalStateException("failed to parse column name: " + fullName, e);
    }
  }

  public static String encode(String fullName) {
    try {
      return URLEncoder.encode(fullName, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String decode(String encoded) {
    try {
      return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  /** @return encoded form of the literal part of path pattern before the first wildcard */
  public static String encodedLiteralPrefix(String pattern) {
    int star = pattern.indexOf('*');
    String literal = star < 0 ? pattern : pattern.substring(0, star);
    return encode(TRANSLATOR.getEscaper().escape(literal));
  }
}
