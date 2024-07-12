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
package cn.edu.tsinghua.iginx.redis.tools;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagKVUtils {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(TagKVUtils.class);

  private static final ColumnKeyTranslator COLUMN_KEY_TRANSLATOR =
      new ColumnKeyTranslator(',', '=', getEscaper());

  private static Escaper getEscaper() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put(',', ',');
    replacementMap.put('=', '=');
    return new Escaper('\\', replacementMap);
  }

  public static String toFullName(String name, Map<String, String> tags) {
    if (tags == null) {
      tags = Collections.emptyMap();
    }
    ColumnKey columnKey = new ColumnKey(name, tags);
    return COLUMN_KEY_TRANSLATOR.translate(columnKey);
  }

  public static Pair<String, Map<String, String>> splitFullName(String fullName) {
    try {
      ColumnKey columnKey = COLUMN_KEY_TRANSLATOR.translate(fullName);
      return new Pair<>(columnKey.getPath(), columnKey.getTags());
    } catch (ParseException e) {
      throw new IllegalStateException("Failed to parse identifier: " + fullName, e);
    }
  }

  public static String getPattern(String name) {
    String escaped = COLUMN_KEY_TRANSLATOR.getEscaper().escape(name);
    return escaped.replaceAll("[?^{}\\[\\]\\\\]", "\\\\$0");
  }

  public static boolean match(Map<String, String> tags, TagFilter tagFilter) {
    switch (tagFilter.getType()) {
      case And:
        return match(tags, (AndTagFilter) tagFilter);
      case Or:
        return match(tags, (OrTagFilter) tagFilter);
      case Base:
        return match(tags, (BaseTagFilter) tagFilter);
      case Precise:
        return match(tags, (PreciseTagFilter) tagFilter);
      case BasePrecise:
        return match(tags, (BasePreciseTagFilter) tagFilter);
      case WithoutTag:
        return match(tags, (WithoutTagFilter) tagFilter);
    }
    return false;
  }

  private static boolean match(Map<String, String> tags, AndTagFilter tagFilter) {
    if (tags == null || tags.isEmpty()) {
      return false;
    }
    List<TagFilter> children = tagFilter.getChildren();
    for (TagFilter child : children) {
      if (!match(tags, child)) {
        return false;
      }
    }
    return true;
  }

  private static boolean match(Map<String, String> tags, OrTagFilter tagFilter) {
    if (tags == null || tags.isEmpty()) {
      return false;
    }
    List<TagFilter> children = tagFilter.getChildren();
    for (TagFilter child : children) {
      if (match(tags, child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean match(Map<String, String> tags, BaseTagFilter tagFilter) {
    if (tags == null || tags.isEmpty()) {
      return false;
    }
    String tagKey = tagFilter.getTagKey();
    String expectedValue = tagFilter.getTagValue();
    if (!tags.containsKey(tagKey)) {
      return false;
    }
    String actualValue = tags.get(tagKey);
    if (!StringUtils.isPattern(expectedValue)) {
      return expectedValue.equals(actualValue);
    } else {
      return Pattern.matches(StringUtils.reformatPath(expectedValue), actualValue);
    }
  }

  private static boolean match(Map<String, String> tags, PreciseTagFilter tagFilter) {
    if (tags == null || tags.isEmpty()) {
      return false;
    }
    List<BasePreciseTagFilter> children = tagFilter.getChildren();
    for (TagFilter child : children) {
      if (match(tags, child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean match(Map<String, String> tags, BasePreciseTagFilter tagFilter) {
    if (tags == null || tags.isEmpty()) {
      return false;
    }
    return tags.equals(tagFilter.getTags());
  }

  private static boolean match(Map<String, String> tags, WithoutTagFilter tagFilter) {
    return tags == null || tags.isEmpty();
  }
}
