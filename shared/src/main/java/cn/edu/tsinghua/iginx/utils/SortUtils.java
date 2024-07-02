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
