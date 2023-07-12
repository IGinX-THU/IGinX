package cn.edu.tsinghua.iginx.influxdb.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import java.util.*;

public class TagFilterUtils {

  public static String transformToFilterStr(TagFilter filter) {
    StringBuilder builder = new StringBuilder();
    transformToFilterStr(filter, builder);
    return builder.toString();
  }

  private static void transformToFilterStr(TagFilter filter, StringBuilder builder) {
    switch (filter.getType()) {
      case And:
        AndTagFilter andFilter = (AndTagFilter) filter;
        for (int i = 0; i < andFilter.getChildren().size(); i++) {
          builder.append('(');
          transformToFilterStr(andFilter.getChildren().get(i), builder);
          builder.append(')');
          if (i != andFilter.getChildren().size() - 1) { // 还不是最后一个
            builder.append(" and ");
          }
        }
        break;
      case Or:
        OrTagFilter orFilter = (OrTagFilter) filter;
        for (int i = 0; i < orFilter.getChildren().size(); i++) {
          builder.append('(');
          transformToFilterStr(orFilter.getChildren().get(i), builder);
          builder.append(')');
          if (i != orFilter.getChildren().size() - 1) { // 还不是最后一个
            builder.append(" or ");
          }
        }
        break;
      case Base:
        BaseTagFilter baseFilter = (BaseTagFilter) filter;
        builder.append("r.").append(baseFilter.getTagKey());
        if (baseFilter.getTagValue().equals("*")) {
          builder.append(" =~ /.+/ ");
        } else {
          builder.append(" == \"").append(baseFilter.getTagValue()).append("\" ");
        }
        break;
        // TODO: case label
      case BasePrecise:
        BasePreciseTagFilter basePreciseTagFilter = (BasePreciseTagFilter) filter;
        Map<String, String> tags = basePreciseTagFilter.getTags();
        builder.append("(");
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          builder.append("(");
          builder.append("r.").append(key);
          if (value.equals("*")) {
            builder.append(" =~ /.+/ ");
          } else {
            builder.append(" == \"").append(value).append("\" ");
          }
          builder.append(")");
          builder.append(" and ");
        }
        int index = builder.lastIndexOf(" and ");
        builder = builder.delete(index, index + 5);
        builder.append(")");
        break;
      case Precise:
        PreciseTagFilter preciseTagFilter = (PreciseTagFilter) filter;
        builder.append('(');
        for (int i = 0; i < preciseTagFilter.getChildren().size(); i++) {
          builder.append('(');
          transformToFilterStr(preciseTagFilter.getChildren().get(i), builder);
          builder.append(')');
          if (i != preciseTagFilter.getChildren().size() - 1) { // 还不是最后一个
            builder.append(" or ");
          }
        }
        builder.append(")");
        break;
      case WithoutTag:
        break;
    }
  }

  public static boolean match(Map<String, String> tags, TagFilter tagFilter) {
    switch (tagFilter.getType()) {
      case Precise:
        return match(tags, (PreciseTagFilter) tagFilter);
      case BasePrecise:
        return match(tags, (BasePreciseTagFilter) tagFilter);
      case WithoutTag:
        return match(tags, (WithoutTagFilter) tagFilter);
      default:
        return true;
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
