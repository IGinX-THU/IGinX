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

package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.parquet.shared.exception.UnsupportedFilterException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProjectUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectUtils.class);

  public static Map<String, DataType> project(Map<String, DataType> schema, TagFilter tagFilter) {
    if (tagFilter == null) {
      return schema;
    }

    Map<String, DataType> result = new HashMap<>();
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      if (TagKVUtils.match(pathWithTags.getValue(), tagFilter)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  public static Map<String, DataType> project(Map<String, DataType> schema, List<String> paths) {
    Map<String, DataType> result = new HashMap<>();
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      for (String path : paths) {
        if (StringUtils.match(pathWithTags.getKey(), path)) {
          result.put(entry.getKey(), entry.getValue());
          break;
        }
      }
    }
    return result;
  }

  public static Filter project(Filter filter, Map<String, DataType> schema) {
    try {
      return project(filter, schema, false);
    } catch (UnsupportedFilterException e) {
      return new BoolFilter(true);
    }
  }

  private static Filter project(Filter filter, Map<String, DataType> schema, boolean notInParent)
      throws UnsupportedFilterException {
    try {
      switch (filter.getType()) {
        case Key:
        case Bool:
          return filter;
        case And:
          return project((AndFilter) filter, schema, notInParent);
        case Or:
          return project((OrFilter) filter, schema, notInParent);
        case Not:
          return project((NotFilter) filter, schema, notInParent);
        case Value:
          return project((ValueFilter) filter, schema);
        case Path:
          return project((PathFilter) filter, schema);
        default:
          throw new UnsupportedFilterException(filter);
      }
    } catch (UnsupportedFilterException e) {
      if (notInParent) {
        throw e;
      } else {
        LOGGER.trace("unsupported filter {} is regarded as true because: {}", filter, e.toString());
        return new BoolFilter(true);
      }
    }
  }

  private static Filter project(AndFilter filter, Map<String, DataType> schema, boolean notInParent)
      throws UnsupportedFilterException {
    List<Filter> filters = new ArrayList<>();
    for (Filter subfilter : filter.getChildren()) {
      filters.add(project(subfilter, schema, notInParent));
    }
    return new AndFilter(filters);
  }

  private static Filter project(OrFilter filter, Map<String, DataType> schema, boolean notInParent)
      throws UnsupportedFilterException {
    List<Filter> filters = new ArrayList<>();
    for (Filter subfilter : filter.getChildren()) {
      filters.add(project(subfilter, schema, notInParent));
    }
    return new OrFilter(filters);
  }

  private static Filter project(NotFilter filter, Map<String, DataType> schema, boolean notInParent)
      throws UnsupportedFilterException {
    return new NotFilter(project(filter.getChild(), schema, true));
  }

  private static Filter project(ValueFilter filter, Map<String, DataType> schema)
      throws UnsupportedFilterException {
    Map<String, DataType> projectedSchema =
        project(schema, Collections.singletonList(filter.getPath()));

    List<Filter> filters = new ArrayList<>();
    for (String projectedField : projectedSchema.keySet()) {
      filters.add(new ValueFilter(projectedField, getNormalOp(filter.getOp()), filter.getValue()));
    }

    if (isAndOp(filter.getOp())) {
      return new AndFilter(filters);
    } else {
      return new OrFilter(filters);
    }
  }

  private static Filter project(PathFilter filter, Map<String, DataType> schema)
      throws UnsupportedFilterException {
    Map<String, DataType> projectedSchemaA =
        project(schema, Collections.singletonList(filter.getPathA()));
    Map<String, DataType> projectedSchemaB =
        project(schema, Collections.singletonList(filter.getPathB()));

    List<Filter> filters = new ArrayList<>();

    if (projectedSchemaA.size() == 1) {
      String projectedFieldA = projectedSchemaA.keySet().iterator().next();
      for (String projectedFieldB : projectedSchemaB.keySet()) {
        filters.add(new PathFilter(projectedFieldA, getNormalOp(filter.getOp()), projectedFieldB));
      }
    } else if (projectedSchemaB.size() == 1) {
      String projectedFieldB = projectedSchemaB.keySet().iterator().next();
      for (String projectedFieldA : projectedSchemaA.keySet()) {
        filters.add(new PathFilter(projectedFieldA, getNormalOp(filter.getOp()), projectedFieldB));
      }
    } else {
      String message =
          String.format(
              "can't compare %s with %s", projectedSchemaA.keySet(), projectedSchemaB.keySet());
      throw new UnsupportedFilterException(message, filter);
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
}
