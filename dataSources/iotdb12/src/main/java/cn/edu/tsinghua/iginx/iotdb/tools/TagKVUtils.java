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
package cn.edu.tsinghua.iginx.iotdb.tools;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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

  public static final String tagNameAnnotation = Config.tagNameAnnotation;

  public static final String tagPrefix = Config.tagPrefix;

  public static final String tagSuffix = Config.tagSuffix;

  public static String toFullName(String path, Map<String, String> tags) {
    path += '.';
    path += tagPrefix;
    if (tags != null && !tags.isEmpty()) {
      TreeMap<String, String> sortedTags = new TreeMap<>(tags);
      StringBuilder pathBuilder = new StringBuilder();
      sortedTags.forEach(
          (tagKey, tagValue) -> {
            pathBuilder
                .append('.')
                .append(tagNameAnnotation)
                .append(tagKey)
                .append('.')
                .append(tagValue);
          });
      path += pathBuilder.toString();
    }
    path += '.';
    path += tagSuffix;
    return path;
  }

  public static Pair<String, Map<String, String>> splitFullName(String fullName) {
    if (!fullName.contains(tagPrefix)) {
      return new Pair<>(fullName, null);
    }

    String[] parts = fullName.split(tagPrefix, 2);
    assert parts.length == 2;
    String name = parts[0].substring(0, parts[0].length() - 1);
    if (!fullName.contains(tagNameAnnotation)) {
      return new Pair<>(name, null);
    }
    parts[1] = parts[1].substring(1, parts[1].length() - 2);
    List<String> tagKVList =
        Arrays.stream(parts[1].split("\\."))
            .map(
                e -> {
                  if (e.startsWith(tagNameAnnotation)) {
                    return e.substring(tagNameAnnotation.length());
                  } else {
                    return e;
                  }
                })
            .collect(Collectors.toList());
    assert tagKVList.size() % 2 == 0;
    Map<String, String> tags = new HashMap<>();
    for (int i = 0; i < tagKVList.size(); i++) {
      if (i % 2 == 0) {
        continue;
      }
      String tagKey = tagKVList.get(i - 1);
      String tagValue = tagKVList.get(i);
      tags.put(tagKey, tagValue);
    }
    return new Pair<>(name, tags);
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
