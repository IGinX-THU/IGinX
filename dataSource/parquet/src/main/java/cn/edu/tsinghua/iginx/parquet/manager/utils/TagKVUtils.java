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

package cn.edu.tsinghua.iginx.parquet.manager.utils;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.ColumnKeyTranslator;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.utils.Escaper;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TagKVUtils {
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
    return toFullName(columnKey);
  }

  public static String toFullName(ColumnKey columnKey) {
    Objects.requireNonNull(columnKey);
    return COLUMN_KEY_TRANSLATOR.translate(columnKey);
  }

  public static ColumnKey splitFullName(String fullName) {
    try {
      return COLUMN_KEY_TRANSLATOR.translate(fullName);
    } catch (ParseException e) {
      throw new IllegalStateException("Failed to parse identifier: " + fullName, e);
    }
  }

  public static boolean match(ColumnKey columnKey, List<String> patterns, TagFilter tagFilter) {
    for (String pattern : patterns) {
      if (tagFilter == null) {
        if (StringUtils.match(columnKey.getPath(), pattern)) {
          return true;
        }
      } else {
        if (StringUtils.match(columnKey.getPath(), pattern)
            && cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils.match(
                columnKey.getTags(), tagFilter)) {
          return true;
        }
      }
    }
    return false;
  }
}
