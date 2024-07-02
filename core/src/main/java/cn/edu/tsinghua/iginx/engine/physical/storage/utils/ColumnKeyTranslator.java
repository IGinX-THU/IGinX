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
package cn.edu.tsinghua.iginx.engine.physical.storage.utils;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.utils.Escaper;
import java.text.ParseException;
import java.util.*;

public class ColumnKeyTranslator {

  private final char tagSeparator;
  private final char kvSeparator;
  private final Escaper escaper;

  public ColumnKeyTranslator(char tagSeparator, char kvSeparator, Escaper escaper) {
    this.tagSeparator = tagSeparator;
    this.kvSeparator = kvSeparator;
    this.escaper = Objects.requireNonNull(escaper);
    if (tagSeparator == kvSeparator) {
      throw new IllegalArgumentException(
          "Tag separator and key-value separator should be different");
    }
    char es = escaper.getEscapePrefix();
    if (es == tagSeparator || es == kvSeparator) {
      throw new IllegalArgumentException(
          "Escape prefix should not be the same as tag separator or key-value separator");
    }
    if (escaper.escape(tagSeparator) == null || escaper.escape(kvSeparator) == null) {
      throw new IllegalArgumentException(
          "Escaper should be able to escape tag separator and key-value separator");
    }
  }

  public Escaper getEscaper() {
    return escaper;
  }

  public String translate(ColumnKey columnKey) {
    StringBuilder sb = new StringBuilder();
    escaper.escape(columnKey.getPath(), sb);
    for (Map.Entry<String, String> entry : columnKey.getTags().entrySet()) {
      sb.append(tagSeparator);
      escaper.escape(entry.getKey(), sb);
      sb.append(kvSeparator);
      escaper.escape(entry.getValue(), sb);
    }
    return sb.toString();
  }

  public ColumnKey translate(String id) throws ParseException {
    List<Map.Entry<Integer, Integer>> tagRanges = getTagRanges(id, 0, id.length());

    int pathStart = tagRanges.get(0).getKey();
    int pathEnd = tagRanges.get(0).getValue();
    String path = escaper.unescape(id, pathStart, pathEnd);
    Map<String, String> tags = new HashMap<>();
    for (int i = 1; i < tagRanges.size(); i++) {
      Map.Entry<Integer, Integer> tagRange = tagRanges.get(i);
      Map.Entry<String, String> tag = parseTag(id, tagRange.getKey(), tagRange.getValue());
      tags.put(tag.getKey(), tag.getValue());
    }

    return new ColumnKey(path, tags);
  }

  private List<Map.Entry<Integer, Integer>> getTagRanges(CharSequence in, int start, int end)
      throws ParseException {
    List<Map.Entry<Integer, Integer>> tagRanges = new ArrayList<>();
    int tagStart = start;
    boolean escaped = false;
    for (int i = start; i < end; i++) {
      if (escaped) {
        escaped = false;
        continue;
      }
      char c = in.charAt(i);
      if (c == escaper.getEscapePrefix()) {
        escaped = true;
      } else if (c == tagSeparator) {
        tagRanges.add(new AbstractMap.SimpleImmutableEntry<>(tagStart, i));
        tagStart = i + 1;
      }
    }
    if (escaped) {
      throw new ParseException("Missing escaped character", end);
    }
    tagRanges.add(new AbstractMap.SimpleImmutableEntry<>(tagStart, end));
    return tagRanges;
  }

  private Map.Entry<String, String> parseTag(String in, int start, int end) throws ParseException {
    int kvSeparatorIndex = -1;

    boolean escaped = false;
    for (int i = start; i < end; i++) {
      if (escaped) {
        escaped = false;
        continue;
      }
      char c = in.charAt(i);
      if (c == escaper.getEscapePrefix()) {
        escaped = true;
      } else if (c == kvSeparator) {
        if (kvSeparatorIndex != -1) {
          throw new ParseException("Multiple key-value separators", i);
        }
        kvSeparatorIndex = i;
      }
    }

    if (escaped) {
      throw new ParseException("Missing escaped character", end);
    }

    if (kvSeparatorIndex == -1) {
      throw new ParseException("Missing key-value separator", end);
    }

    String key = escaper.unescape(in, start, kvSeparatorIndex);
    String value = escaper.unescape(in, kvSeparatorIndex + 1, end);

    return new AbstractMap.SimpleImmutableEntry<>(key, value);
  }
}
