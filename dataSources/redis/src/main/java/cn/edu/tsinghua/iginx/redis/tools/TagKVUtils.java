/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    return escapeRedisSpecialCharInPattern(escaped);
  }

  public static String escapeRedisSpecialCharInPattern(String name) {
    return name.replaceAll("[?^{}\\[\\]\\\\]", "\\\\$0");
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
