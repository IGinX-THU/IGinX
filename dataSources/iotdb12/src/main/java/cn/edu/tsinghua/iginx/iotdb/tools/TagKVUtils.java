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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.iotdb.tools;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 由于IGinX支持除了反引号和换行符之外的所有字符，因此需要TagKVUtils的实现
public class TagKVUtils {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(TagKVUtils.class);

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
