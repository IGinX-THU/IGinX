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
package cn.edu.tsinghua.iginx.relational.tools;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.*;

import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

// TODO: 由于IGinX支持除了反引号和换行符之外的所有字符，因此需要TagKVUtils的实现
public class TagKVUtils {

  public static Pair<String, Map<String, String>> splitFullName(String fullName) {
    if (!fullName.contains(TAGKV_SEPARATOR)) {
      return new Pair<>(fullName, null);
    }

    int separator_index = fullName.indexOf(TAGKV_SEPARATOR);
    String name = fullName.substring(0, separator_index);
    String[] parts = fullName.substring(separator_index + 1).split(TAGKV_SEPARATOR);

    Map<String, String> tags = new HashMap<>();
    for (int i = 0; i < parts.length; i++) {
      int equal_index = parts[i].indexOf(TAGKV_EQUAL);
      String tagKey = parts[i].substring(0, equal_index);
      String tagValue = parts[i].substring(equal_index + 1);
      tags.put(tagKey, tagValue);
    }
    return new Pair<>(name, tags);
  }

  public static String toFullName(String name, Map<String, String> tags) {
    if (tags != null && !tags.isEmpty()) {
      TreeMap<String, String> sortedTags = new TreeMap<>(tags);
      StringBuilder pathBuilder = new StringBuilder();
      sortedTags.forEach(
          (tagKey, tagValue) ->
              pathBuilder
                  .append(TAGKV_SEPARATOR)
                  .append(tagKey)
                  .append(TAGKV_EQUAL)
                  .append(tagValue));
      name += pathBuilder.toString();
    }
    return name;
  }
}
