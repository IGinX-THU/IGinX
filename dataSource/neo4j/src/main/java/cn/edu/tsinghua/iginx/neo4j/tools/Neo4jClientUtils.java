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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.SEPARATOR;
import static cn.edu.tsinghua.iginx.neo4j.tools.Constants.DATABASE_PREFIX;
import static cn.edu.tsinghua.iginx.neo4j.tools.Constants.IDENTITY_PROPERTY_NAME;
import static cn.edu.tsinghua.iginx.neo4j.tools.DataTransformer.fromStringDataType;
import static cn.edu.tsinghua.iginx.neo4j.tools.Neo4jSchema.getQuoteName;
import static cn.edu.tsinghua.iginx.neo4j.tools.RegexEscaper.escapeRegex;
import static cn.edu.tsinghua.iginx.neo4j.tools.TagKVUtils.splitFullName;
import static org.neo4j.driver.Values.parameters;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.neo4j.Neo4jStorage;
import cn.edu.tsinghua.iginx.neo4j.entity.Column;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.value.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @description : Neo4j 数据库操作工具类 */
public class Neo4jClientUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStorage.class);

  public static boolean checkConstraintExists(Session session, String label, String property) {
    Result result =
        session.run(
            "SHOW CONSTRAINTS WHERE type = 'UNIQUENESS' AND labelsOrTypes = [$label] AND properties = [$property]",
            parameters("label", label, "property", property));
    return result.hasNext();
  }

  public static boolean checkAndCreateUniqueConstraint(
      Session session, String label, String property) {
    boolean exists = checkConstraintExists(session, label, property); // 如果有结果返回，说明约束已存在
    if (!exists) {
      String query =
          String.format(
              "CREATE CONSTRAINT FOR (n:%s) REQUIRE n.%s IS UNIQUE",
              getQuoteName(label), getQuoteName(property));
      try {
        Result result = session.run(query);
        result.consume(); // 确保操作完成
        return true;
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

  public static boolean bulkInsert(
      Session session, String label, String idProperty, Collection<Map<String, Object>> nodes) {
    try {
      // 批量插入数据（使用 UNWIND + MERGE 防止重复）
      String insertQuery =
          String.format(
              "UNWIND $nodes AS node "
                  + "MERGE (n:%s {%s: node.%s}) "
                  + "ON CREATE SET n += node.properties "
                  + "ON MATCH SET n += node.properties",
              getQuoteName(label), idProperty, idProperty);
      Map<String, Object> params = new HashMap();
      params.put(
          "nodes",
          nodes.stream()
              .map(
                  node -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put(idProperty, node.get(idProperty));
                    map.put("properties", node);
                    return map;
                  })
              .collect(Collectors.toList()));

      session.run(insertQuery, params).consume();
      LOGGER.info("bulk insert " + label + "success : " + nodes.size());
      return true;
    } catch (Exception e) {
      LOGGER.error("bulk insert error: ", e);
      return false;
    }
  }

  public static Map<String, Map<String, String>> determinePaths(
      Session session,
      Collection<String> patterns,
      TagFilter tagFilter,
      String databaseName,
      boolean dummyOnly) {
    Map<String, Map<String, String>> result = new HashMap<>();

    Map<String, Map<String, String>> labelToProperties =
        splitAndMergeQueryPatterns(session, patterns, databaseName, dummyOnly);
    // iterate labels
    for (Map.Entry<String, Map<String, String>> entry : labelToProperties.entrySet()) {
      // iterate properties
      for (Map.Entry<String, String> entry1 : entry.getValue().entrySet()) {
        Pair<String, Map<String, String>> propertyToTags =
            splitFullName(entry.getKey() + SEPARATOR + entry1.getKey());
        if (tagFilter != null
            && !cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils.match(
                propertyToTags.getV(), tagFilter)) {
          continue;
        }
        result
            .computeIfAbsent(entry.getKey(), k -> new HashMap<>())
            .computeIfAbsent(entry1.getKey(), k -> entry1.getValue());
      }
    }
    return result;
  }

  public static Map<String, Map<String, String>> splitAndMergeQueryPatterns(
      Session session, Collection<String> patterns, String databaseName, boolean dummyOnly) {
    // label name -> (property names -> property type)
    // 1 -> n
    Map<String, Map<String, String>> labelToProperties = new HashMap<>();
    String label;
    String property;

    if (patterns == null || patterns.isEmpty()) {
      patterns = new ArrayList<>();
      patterns.add("*");
    }

    String prefix =
        org.apache.commons.lang3.StringUtils.isEmpty(databaseName)
            ? "(unit.*\\.)?"
            : databaseName + "\\" + SEPARATOR;
    for (String pattern : patterns) {
      if (pattern.equals("*") || pattern.equals("*.*")) {
        label = prefix + ".*";
        property = ".*";
      } else if (pattern.split("\\" + SEPARATOR).length == 1) { // REST 查询的路径中可能不含 .
        label = prefix + escapeRegex(pattern).replace("\\*", ".*");
        property = ".*";
      } else {
        Neo4jSchema schema = new Neo4jSchema(pattern, false);
        label = schema.getLabelName();
        property = schema.getPropertyName();
        boolean propertyEqualsStar = property.startsWith("*");
        boolean labelContainsStar = label.contains("*");
        label = escapeRegex(label).replace("\\*", ".*");
        if (propertyEqualsStar && !label.endsWith("*")) {
          label = label + "(\\..+)?";
        }
        label = prefix + label;
        property = escapeRegex(property).replace("\\*", ".*");
      }

      if (!property.endsWith("*")) {
        property += ".*"; // 匹配 tagKV
      }

      Map<String, String> keyPropertyMap = new HashMap<>();
      List<LabelProperty> columnFieldList = getProperties(session, label, property);
      for (LabelProperty labelProperty : columnFieldList) {
        String curlabelName = validateLabelName(labelProperty.getLabelName());

        if (dummyOnly && !isDummy(curlabelName)) {
          continue;
        }
        if (!keyPropertyMap.containsKey(curlabelName)) {
          String keyProperty = getUniqueConstraintName(session, curlabelName);
          LOGGER.info("-------------------keyProperty:" + keyProperty);
          keyPropertyMap.put(curlabelName, keyProperty);
        }

        String curPropertyNames = validatePropertyName(labelProperty.getPropertyName());
        if (curPropertyNames.equals(IDENTITY_PROPERTY_NAME)) {
          continue;
        }
        if (keyPropertyMap.get(curlabelName) != null
            && keyPropertyMap.get(curlabelName).equals(curPropertyNames)) {
          continue;
        }
        labelToProperties
            .computeIfAbsent(curlabelName, k -> new HashMap<>())
            .put(curPropertyNames, labelProperty.getPropertyType());
      }
    }
    return labelToProperties;
  }

  public static String trimPrefix(String labelName) {
    return labelName.replaceFirst("^" + DATABASE_PREFIX + "[^\\.]*\\.", "");
  }

  public static boolean isDummy(String labelName) {
    return !labelName.startsWith(DATABASE_PREFIX);
  }

  public static String validateLabelName(String labelName) {
    return labelName
        .replaceFirst("^\"", "")
        .replaceFirst("\"$", "")
        .replaceFirst("^:`", "")
        .replaceFirst("`$", "");
  }

  public static String validatePropertyName(String propertyName) {
    return propertyName.replaceFirst("^\"", "").replaceFirst("\"$", "");
  }

  public static List<LabelProperty> getProperties(
      Session session, final String labelPattern, final String propertyPattern) {
    try {
      String query =
          "CALL db.schema.nodeTypeProperties() "
              + "YIELD nodeType, propertyName, propertyTypes "
              + "WHERE nodeType =~ $labelPattern and propertyName=~ $propertyPattern "
              + "RETURN DISTINCT nodeType as labelName, propertyName AS propertyName, propertyTypes AS propertyType";
      List<Record> records =
          session.readTransaction(
              tx -> {
                Result result =
                    tx.run(
                        query,
                        parameters(
                            "labelPattern",
                            ":`" + labelPattern + "`",
                            "propertyPattern",
                            propertyPattern));
                return result.list();
              });

      List<LabelProperty> list = new ArrayList<>();
      for (Record record : records) {
        list.add(
            new LabelProperty(
                validateLabelName(record.get("labelName").asString()),
                validatePropertyName(record.get("propertyName").asString()),
                record.get("propertyType").asList().get(0).toString()));
      }

      return list;
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new ArrayList<>();
    }
  }

  public static String getUniqueConstraintName(Session session, String label) {
    try {
      String query = "SHOW CONSTRAINTS WHERE type = 'UNIQUENESS' AND labelsOrTypes = [$label] ";
      List<Record> records =
          session.readTransaction(
              tx -> {
                Result result = tx.run(query, parameters("label", label));
                return result.list();
              });
      List<LabelProperty> propertiesList = getProperties(session, label, ".*");
      Map<String, String> properties = new HashMap<>();
      for (LabelProperty labelProperty : propertiesList) {
        properties.put(labelProperty.getPropertyName(), labelProperty.getPropertyType());
      }
      for (Record record : records) {
        String property = record.get("properties").asList().get(0).toString();
        if ("Long".equalsIgnoreCase(properties.get(property))) {
          return property;
        }
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return null;
    }
  }

  public static List<Column> query(
      Session session,
      String label,
      Map<String, String> properties,
      Filter filter,
      boolean isDummy) {
    LOGGER.info("query: " + label + ", " + properties + ", " + filter + ", " + isDummy);
    try {
      String expr = "";
      String quotedLabel = getQuoteName(label);

      String keyProperty = null;
      if (isDummy) {
        keyProperty = getUniqueConstraintName(session, label);
      }

      if (!FilterUtils.filterContainsType(
          Arrays.asList(FilterType.Value, FilterType.Path, FilterType.In), filter)) {
        if (isDummy) {
          if (keyProperty == null) {
            expr =
                new FilterTransformer("id(" + quotedLabel + ")", label)
                    .toString(FilterUtils.expandFilter(filter, "id(" + quotedLabel + ")"));
          } else {
            expr =
                new FilterTransformer(quotedLabel + ".`" + keyProperty + "`", label)
                    .toString(
                        FilterUtils.expandFilter(filter, quotedLabel + ".`" + keyProperty + "`"));
          }
        } else {
          expr =
              new FilterTransformer(quotedLabel + ".`" + IDENTITY_PROPERTY_NAME + "`", label)
                  .toString(
                      FilterUtils.expandFilter(
                          filter, quotedLabel + ".`" + IDENTITY_PROPERTY_NAME + "`"));
        }
        if (StringUtils.isNotEmpty(expr)) {
          expr = "WHERE " + expr + " ";
        }
      }

      StringBuilder r = new StringBuilder();
      if (isDummy) {
        if (keyProperty == null) {
          r.append("id(" + quotedLabel + ") as ").append(IDENTITY_PROPERTY_NAME);
        } else {
          r.append(quotedLabel + ".`")
              .append(keyProperty)
              .append("` as ")
              .append(IDENTITY_PROPERTY_NAME);
          properties.remove(keyProperty);
        }
      } else {
        r.append(quotedLabel + ".`")
            .append(IDENTITY_PROPERTY_NAME)
            .append("` as ")
            .append(IDENTITY_PROPERTY_NAME);
        properties.remove(IDENTITY_PROPERTY_NAME);
      }
      for (String property : properties.keySet()) {
        r.append("," + quotedLabel + ".")
            .append(getQuoteName(property))
            .append(" as ")
            .append(getQuoteName(property));
      }

      String query = "Match (" + quotedLabel + ":" + quotedLabel + ") " + expr + "RETURN " + r;
      LOGGER.info("query: {}", query);
      List<Record> records =
          session.readTransaction(
              tx -> {
                Result result = tx.run(query);
                return result.list();
              });

      List<Column> columns = new ArrayList<>();
      for (String property : properties.keySet()) {
        String pathName = label + SEPARATOR + property;
        DataType type = fromStringDataType(properties.get(property).toUpperCase());
        Map<Long, Object> data = new HashMap<>();
        for (Record record : records) {
          if (record.get(property) != null
              && !record.get(property).isNull()
              && !(record.get(property) instanceof NullValue)) {
            data.put(
                record.get(IDENTITY_PROPERTY_NAME).asLong(),
                "String".equals(properties.get(property))
                    ? record.get(property).asString()
                    : transform(record.get(property)));
          }
        }
        Column c = new Column(trimPrefix(pathName), type, data);
        columns.add(c);
      }

      return columns;
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      return new ArrayList<>();
    }
  }

  public static Object transform(Object value) {
    if (value instanceof org.neo4j.driver.internal.value.IntegerValue) {
      return ((IntegerValue) value).asLong();
    } else if (value instanceof org.neo4j.driver.internal.value.FloatValue) {
      return ((FloatValue) value).asDouble();
    } else if (value instanceof org.neo4j.driver.internal.value.BooleanValue) {
      return ((BooleanValue) value).asBoolean();
    } else {
      return value.toString();
    }
  }

  public static boolean clearDatabase(Session session) {
    clearConstraint(session);
    //    session.run("MATCH ()-[r]->() CALL { WITH r DELETE r } IN TRANSACTIONS OF 1000
    // ROWS;").consume();
    String query =
        "CALL apoc.periodic.iterate(\"MATCH (n) RETURN n\", \"DETACH DELETE n\",  {batchSize: 5000})";
    LOGGER.info("query: {}", query);
    session.run(query).consume();
    return true;
  }

  public static void clearConstraint(Session session) {
    Result constraintsResult = session.run("SHOW CONSTRAINTS");

    constraintsResult.stream()
        .forEach(
            record -> {
              String constraintName = record.get("name").asString();
              String dropQuery = String.format("DROP CONSTRAINT %s", constraintName);

              try {
                session.run(dropQuery).consume();
              } catch (Exception e) {
              }
            });
  }

  public static boolean removeProperties(
      Session session, String label, Collection<String> properties) {
    if (properties == null || properties.size() < 1) return false;
    StringBuilder q = new StringBuilder("MATCH (n:`").append(label).append("`) REMOVE ");
    for (String property : properties) {
      q.append("n.`").append(property).append("`,");
    }
    q.deleteCharAt(q.length() - 1);
    LOGGER.info("query: {}", q);
    session.run(q.toString()).consume();
    return true;
  }

  public static boolean deleteByRange(
      Session session, String label, Collection<String> properties, KeyRange keyRange) {
    if (properties == null || properties.size() < 1) return false;
    StringBuilder q =
        new StringBuilder("MATCH (n:`")
            .append(label)
            .append("`) WHERE n.")
            .append(IDENTITY_PROPERTY_NAME)
            .append(" >=")
            .append(keyRange.getActualBeginKey())
            .append(" AND n.")
            .append(IDENTITY_PROPERTY_NAME)
            .append(" <=")
            .append(keyRange.getActualEndKey())
            .append(" SET ");
    for (String property : properties) {
      q.append("n.`").append(property).append("`=null,");
    }
    q.deleteCharAt(q.length() - 1);
    LOGGER.info("query: {}", q);
    session.run(q.toString()).consume();
    return true;
  }
}
