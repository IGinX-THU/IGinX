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
package cn.edu.tsinghua.iginx.neo4j.tools;

import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.List;
import java.util.Map;

public class PathUtils {

  public static Pair<String, String> getLabelAndPropertyByPath(
      String path, Map<String, String> tags, boolean isDummy) {
    Neo4jSchema schema = new Neo4jSchema(path, isDummy);
    String labelName = schema.getLabelName();
    String propertyName = schema.getPropertyName();
    propertyName = TagKVUtils.toFullName(propertyName, tags);
    return new Pair<>(labelName, propertyName);
  }

  public static boolean match(String path, List<String> patterns) {
    for (String pattern : patterns) {
      if (path.equals(pattern)) {
        return true;
      } else if (path.contains("*") && pattern.matches(StringUtils.reformatPath(path))) {
        return true;
      } else if (pattern.contains("*") && path.matches(StringUtils.reformatPath(pattern))) {
        return true;
      } else if (path.matches(StringUtils.toRegexExpr(pattern + ".*"))) {
        return true;
      }
    }
    return false;
  }
}
