/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ProjectUtils {
  public static boolean match(String fullPath, List<Pattern> patterns, TagFilter tagFilter) {
    if (tagFilter == null) {
      return match(fullPath, patterns);
    }
    for (Pattern pattern : patterns) {
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(fullPath);
      if (pattern.matcher(pair.getK()).matches() && TagKVUtils.match(pair.getV(), tagFilter)) {
        return true;
      }
    }
    return false;
  }

  public static boolean match(String fullPath, List<Pattern> patterns) {
    for (Pattern pattern : patterns) {
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(fullPath);
      if (pattern.matcher(pair.getK()).matches()) {
        return true;
      }
    }
    return false;
  }

  public static Set<String> project(Set<String> schema, List<String> paths, TagFilter tagFilter) {
    List<Pattern> patterns =
        paths.stream().map(ProjectUtils::compliePattern).collect(Collectors.toList());

    Set<String> result = new HashSet<>();
    for (String fieldName : schema) {
      if (ProjectUtils.match(fieldName, patterns, tagFilter)) {
        result.add(fieldName);
      }
    }
    return result;
  }

  public static Set<String> project(Set<String> schema, TagFilter tagFilter) {
    if (tagFilter == null) {
      return new HashSet<>(schema);
    }
    Set<String> result = new HashSet<>();
    for (String fieldName : schema) {
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(fieldName);
      if (TagKVUtils.match(pair.getV(), tagFilter)) {
        result.add(fieldName);
      }
    }
    return result;
  }

  public static Set<String> project(Set<String> schema, List<String> paths) {
    List<Pattern> patterns =
        paths.stream().map(ProjectUtils::compliePattern).collect(Collectors.toList());
    Set<String> result = new HashSet<>();
    for (String fieldName : schema) {
      if (ProjectUtils.match(fieldName, patterns)) {
        result.add(fieldName);
      }
    }
    return result;
  }

  public static Pattern compliePattern(String wildcard) {
    return Pattern.compile(StringUtils.reformatPath(wildcard));
  }

  public static Filter project(Filter filter, Set<String> fields) {
    switch (filter.getType()) {
      case Key:
      case Bool:
        return filter;
      case And:
        return project((AndFilter) filter, fields);
      case Or:
        return project((OrFilter) filter, fields);
      case Not:
        return project((NotFilter) filter, fields);
      case Value:
        return project((ValueFilter) filter, fields);
      case Path:
        return project((PathFilter) filter, fields);
      default:
        throw new IllegalStateException("unsupported filter: " + filter);
    }
  }

  public static Filter project(AndFilter filter, Set<String> fields) {
    List<Filter> filters = new ArrayList<>();
    for (Filter subfilter : filter.getChildren()) {
      filters.add(project(subfilter, fields));
    }
    return new AndFilter(filters);
  }

  public static Filter project(OrFilter filter, Set<String> fields) {
    List<Filter> filters = new ArrayList<>();
    for (Filter subfilter : filter.getChildren()) {
      filters.add(project(subfilter, fields));
    }
    return new OrFilter(filters);
  }

  public static Filter project(NotFilter filter, Set<String> fields) {
    return new NotFilter(project(filter.getChild(), fields));
  }

  public static Filter project(ValueFilter filter, Set<String> fields) {
    Set<String> projectedFields = project(fields, Collections.singletonList(filter.getPath()));

    List<Filter> filters = new ArrayList<>();
    for (String projectedField : projectedFields) {
      filters.add(new ValueFilter(projectedField, getNormalOp(filter.getOp()), filter.getValue()));
    }

    if (isAndOp(filter.getOp())) {
      return new AndFilter(filters);
    } else {
      return new OrFilter(filters);
    }
  }

  public static Filter project(PathFilter filter, Set<String> fields) {
    Set<String> projectedFieldAs = project(fields, Collections.singletonList(filter.getPathA()));
    Set<String> projectedFieldBs = project(fields, Collections.singletonList(filter.getPathB()));

    List<Filter> filters = new ArrayList<>();

    if (projectedFieldAs.size() == 1) {
      String projectedFieldA = projectedFieldAs.iterator().next();
      for (String projectedFieldB : projectedFieldBs) {
        filters.add(new PathFilter(projectedFieldA, getNormalOp(filter.getOp()), projectedFieldB));
      }
    }
    if (projectedFieldBs.size() == 1) {
      String projectedFieldB = projectedFieldBs.iterator().next();
      for (String projectedFieldA : projectedFieldAs) {
        filters.add(new PathFilter(projectedFieldA, getNormalOp(filter.getOp()), projectedFieldB));
      }
    }

    if (isAndOp(filter.getOp())) {
      return new AndFilter(filters);
    } else {
      return new OrFilter(filters);
    }
  }

  private static boolean isAndOp(Op op) {
    switch (op) {
      case GE:
      case G:
      case LE:
      case L:
      case E:
      case NE:
      case LIKE:
        return false;
      case GE_AND:
      case G_AND:
      case LE_AND:
      case L_AND:
      case E_AND:
      case NE_AND:
      case LIKE_AND:
        return true;
      default:
        throw new IllegalStateException("unsupported op: " + op);
    }
  }

  private static Op getNormalOp(Op op) {
    switch (op) {
      case GE:
      case GE_AND:
        return Op.GE;
      case G:
      case G_AND:
        return Op.G;
      case LE:
      case LE_AND:
        return Op.LE;
      case L:
      case L_AND:
        return Op.L;
      case E:
      case E_AND:
        return Op.E;
      case NE:
      case NE_AND:
        return Op.NE;
      case LIKE:
      case LIKE_AND:
        return Op.LIKE;
      default:
        throw new IllegalStateException("unsupported op: " + op);
    }
  }

  public static boolean isMatchAll(List<String> paths) {
    Pattern pattern = Pattern.compile("[*]+(.[*]+)*");
    return paths.stream().anyMatch(s -> pattern.matcher(s).matches());
  }
}
