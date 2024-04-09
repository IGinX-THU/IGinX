package cn.edu.tsinghua.iginx.engine.physical.storage.utils;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public class TagKVUtils {

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

  public static String toColumnName(String name, Map<String, String> tags) {
    if (tags == null || tags.isEmpty()) {
      return name;
    } else {
      StringBuilder builder = new StringBuilder();
      builder.append(name);
      builder.append('{');
      TreeMap<String, String> treeMap = new TreeMap<>(tags);

      int cnt = 0;
      for (String key : treeMap.keySet()) {
        if (cnt != 0) {
          builder.append(',');
        }
        builder.append(key);
        builder.append("=");
        builder.append(treeMap.get(key));
        cnt++;
      }
      builder.append('}');
      return builder.toString();
    }
  }

  public static Pair<String, Map<String, String>> fromColumnName(String fullName) {
    int index = fullName.indexOf('{');
    if (index == -1) {
      return new Pair<>(fullName, Collections.emptyMap());
    } else {
      String name = fullName.substring(0, index);
      String[] tagKVs = fullName.substring(index + 1, fullName.length() - 1).split(",");
      Map<String, String> tags = new HashMap<>();
      for (String tagKV : tagKVs) {
        String[] KV = tagKV.split("=", 2);
        tags.put(KV[0], KV[1]);
      }
      return new Pair<>(name, tags);
    }
  }
}
