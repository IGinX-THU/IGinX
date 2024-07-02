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
package cn.edu.tsinghua.iginx.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SortUtils {

  // 适用于查询类请求和删除类请求，因为其 paths 可能带有 *
  public static List<String> mergeAndSortPaths(List<String> paths) {
    if (paths.stream().anyMatch(x -> x.equals("*"))) {
      List<String> tempPaths = new ArrayList<>();
      tempPaths.add("*");
      return tempPaths;
    }
    List<String> prefixes =
        paths.stream()
            .filter(x -> x.contains("*"))
            .map(x -> x.substring(0, x.indexOf("*")))
            .collect(Collectors.toList());
    if (prefixes.isEmpty()) {
      Collections.sort(paths);
      return paths;
    }
    List<String> mergedPaths = new ArrayList<>();
    for (String path : paths) {
      if (path.contains("*")) {
        mergedPaths.add(path);
      } else {
        boolean skip = false;
        for (String prefix : prefixes) {
          if (path.startsWith(prefix)) {
            skip = true;
            break;
          }
        }
        if (skip) {
          continue;
        }
        mergedPaths.add(path);
      }
    }
    mergedPaths.sort(
        Comparator.comparing(o -> o.contains("*") ? o.substring(0, o.indexOf("*")) : o));
    return mergedPaths;
  }
}
