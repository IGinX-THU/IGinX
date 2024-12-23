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
package cn.edu.tsinghua.iginx.vectordb.tools;

import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.*;
import static cn.edu.tsinghua.iginx.vectordb.tools.Constants.MILVUS_VECTOR_FIELD_NAME;
import static cn.edu.tsinghua.iginx.vectordb.tools.DataTransformer.objToDeterminedType;
import static cn.edu.tsinghua.iginx.vectordb.tools.NameUtils.getPathAndVersion;
import static cn.edu.tsinghua.iginx.vectordb.tools.TagKVUtils.splitFullName;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.vectordb.entity.Column;
import cn.edu.tsinghua.iginx.vectordb.support.PathSystem;
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 实现利用MilvusClient操作数据库的工具类 */
public class MilvusClientUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MilvusClientUtils.class);
  private static final boolean isDummyEscape = true;

  public static List<String> listDatabase(MilvusClientV2 client) {
    List<String> list = client.listDatabases().getDatabaseNames();
    List<String> result = new ArrayList<>();
    list.forEach(
        s -> {
          if (isDummy(s) && !isDummyEscape) {
            result.add(s);
          } else {
            result.add(NameUtils.unescape(s));
          }
        });
    return result;
  }

  public static List<String> listCollections(MilvusClientV2 client, String databaseName) {
    try {
      useDatabase(client, databaseName);
    } catch (InterruptedException | UnsupportedEncodingException e) {
      return new ArrayList<>();
    }
    List<String> list = client.listCollections().getCollectionNames();
    List<String> result = new ArrayList<>();
    list.forEach(
        s -> {
          if (isDummy(databaseName) && !isDummyEscape) {
            result.add(s);
          } else {
            result.add(NameUtils.unescape(s));
          }
        });
    return result;
  }

  public static boolean isDummy(String dbName) {
    return !dbName.startsWith(DATABASE_PREFIX);
  }

  public static Map<String, DataType> getCollectionPaths(
      MilvusClientV2 client, String databaseName, String collectionName) {
    DescribeCollectionResp resp =
        client.describeCollection(
            DescribeCollectionReq.builder().collectionName(collectionName).build());
    Map<String, DataType> paths = new HashMap<>();
    Set<String> fields = new HashSet<>();
    if (isDummy(databaseName)) {
      resp.getCollectionSchema()
          .getFieldSchemaList()
          .forEach(
              fieldSchema -> {
                if (!fieldSchema.getIsPrimaryKey()
                    && !fieldSchema.getName().equals(MILVUS_VECTOR_FIELD_NAME)) {
                  paths.put(
                      PathUtils.getPathEscaped(databaseName, collectionName, fieldSchema.getName()),
                      DataTransformer.fromMilvusDataType(fieldSchema.getDataType()));
                  fields.add(fieldSchema.getName());
                }
              });

      // 处理动态字段
      if (resp.getEnableDynamicField()) {
        String filter;
        if (resp.getCollectionSchema().getField(resp.getPrimaryFieldName()).getDataType()
            == io.milvus.v2.common.DataType.VarChar) {
          filter = resp.getPrimaryFieldName() + ">=''";
        } else {
          filter = resp.getPrimaryFieldName() + ">=0";
        }
        QueryResp queryResp =
            client.query(
                QueryReq.builder()
                    .outputFields(Arrays.asList(MILVUS_DYNAMIC_FIELD_NAME))
                    .collectionName(collectionName)
                    .filter(filter)
                    .limit(MILVUS_DYNAMIC_TEST_SIZE)
                    .build());
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
          Map<String, Object> entity = result.getEntity();
          for (String key : entity.keySet()) {
            if (!fields.contains(key) && !key.equals(resp.getPrimaryFieldName())) {
              fields.add(key);
              paths.put(
                  PathUtils.getPathEscaped(databaseName, collectionName, key),
                  DataTransformer.fromObject(entity.get(key)));
            }
          }
        }
      }

    } else {
      resp.getCollectionSchema()
          .getFieldSchemaList()
          .forEach(
              fieldSchema -> {
                if (fieldSchema.getName().equals(MILVUS_DATA_FIELD_NAME)) {
                  paths.put(
                      PathUtils.getPathEscaped(databaseName, collectionName, ""),
                      DataTransformer.fromMilvusDataType(fieldSchema.getDataType()));
                }
              });
    }
    return paths;
  }

  public static void doCreateDatabase(MilvusClientV2 client, String databaseName) {
    client.createDatabase(CreateDatabaseReq.builder().databaseName(databaseName).build());
  }

  public static void useDatabase(MilvusClientV2 client, String databaseName)
      throws InterruptedException, UnsupportedEncodingException {
    if (isDummy(databaseName) && !isDummyEscape) {
    } else {
      databaseName = NameUtils.escape(databaseName);
    }

    try {
      client.useDatabase(databaseName);
    } catch (InterruptedException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      createDatabase(client, databaseName);
      client.useDatabase(databaseName);
    }
  }

  public static void createDatabase(MilvusClientV2 client, String databaseName) {
    doCreateDatabase(client, databaseName);
  }

  public static void createCollection(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      DataType idType,
      Set<String> fieldsToAdd,
      Map<String, DataType> fieldTypes,
      String vectorFieldName)
      throws InterruptedException, UnsupportedEncodingException {
    useDatabase(client, databaseName);
    io.milvus.v2.common.DataType milvusIdType = DataTransformer.toMilvusDataType(idType);
    CreateCollectionReq.CreateCollectionReqBuilder builder =
        CreateCollectionReq.builder()
            .idType(milvusIdType)
            .collectionName(NameUtils.escape(collectionName))
            .consistencyLevel(ConsistencyLevel.STRONG)
            .dimension(DEFAULT_DIMENSION);

    CreateCollectionReq.CollectionSchema schema =
        CreateCollectionReq.CollectionSchema.builder().enableDynamicField(true).build();
    schema.addField(
        AddFieldReq.builder()
            .fieldName(MILVUS_PRIMARY_FIELD_NAME)
            .isPrimaryKey(true)
            .dataType(milvusIdType)
            .build());
    schema.addField(
        AddFieldReq.builder()
            .fieldName(vectorFieldName)
            .dataType(io.milvus.v2.common.DataType.FloatVector)
            .dimension(DEFAULT_DIMENSION)
            .build());

    if (fieldsToAdd != null && !fieldsToAdd.isEmpty()) {
      for (String fieldName : fieldsToAdd) {
        schema.addField(
            AddFieldReq.builder()
                .fieldName(NameUtils.escape(fieldName))
                .dataType(DataTransformer.toMilvusDataType(fieldTypes.get(fieldName)))
                .build());
      }
    }

    List<IndexParam> indexes = new ArrayList<>();
    Map<String, Object> extraParams = new HashMap<>();
    extraParams.put("nlist", MILVUS_INDEX_PARAM_NLIST);
    indexes.add(
        IndexParam.builder()
            .fieldName(vectorFieldName)
            .indexType(DEFAULT_INDEX_TYPE)
            .metricType(DEFAULT_METRIC_TYPE)
            .extraParams(extraParams)
            .build());

    client.createCollection(
        builder
            .collectionSchema(schema)
            .primaryFieldName(MILVUS_PRIMARY_FIELD_NAME)
            .vectorFieldName(vectorFieldName)
            .indexParams(indexes)
            .build());
  }

  public static void createDynamicCollection(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      DataType idType,
      Set<String> fieldsToAdd,
      Map<String, DataType> fieldTypes,
      String vectorFieldName)
      throws InterruptedException, UnsupportedEncodingException {
    useDatabase(client, databaseName);
    io.milvus.v2.common.DataType milvusIdType = DataTransformer.toMilvusDataType(idType);
    CreateCollectionReq.CreateCollectionReqBuilder builder =
        CreateCollectionReq.builder()
            .idType(milvusIdType)
            .collectionName(NameUtils.escape(collectionName))
            .consistencyLevel(ConsistencyLevel.STRONG)
            .enableDynamicField(true)
            .dimension(DEFAULT_DIMENSION);

    CreateCollectionReq.CollectionSchema schema =
        CreateCollectionReq.CollectionSchema.builder().enableDynamicField(true).build();
    schema.addField(
        AddFieldReq.builder()
            .fieldName(MILVUS_PRIMARY_FIELD_NAME)
            .isPrimaryKey(true)
            .dataType(milvusIdType)
            .build());
    schema.addField(
        AddFieldReq.builder()
            .fieldName(vectorFieldName)
            .dataType(io.milvus.v2.common.DataType.FloatVector)
            .dimension(DEFAULT_DIMENSION)
            .build());

    List<IndexParam> indexes = new ArrayList<>();
    Map<String, Object> extraParams = new HashMap<>();
    extraParams.put("nlist", MILVUS_INDEX_PARAM_NLIST);
    indexes.add(
        IndexParam.builder()
            .fieldName(vectorFieldName)
            .indexType(DEFAULT_INDEX_TYPE)
            .metricType(DEFAULT_METRIC_TYPE)
            .extraParams(extraParams)
            .build());

    client.createCollection(
        builder
            .collectionSchema(schema)
            .primaryFieldName(MILVUS_PRIMARY_FIELD_NAME)
            .vectorFieldName(vectorFieldName)
            .indexParams(indexes)
            .build());
  }

  public static void createCollection(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      DataType idType,
      DataType fieldType,
      PathSystem pathSystem)
      throws UnsupportedEncodingException {
    doCreateCollection(client, databaseName, collectionName, idType, fieldType, pathSystem);
  }

  public static void doCreateCollection(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      DataType idType,
      DataType fieldType,
      PathSystem pathSystem)
      throws UnsupportedEncodingException {
    String escapedCollectionName = NameUtils.escape(collectionName);
    CreateCollectionReq.CreateCollectionReqBuilder builder =
        CreateCollectionReq.builder()
            .idType(DataTransformer.toMilvusDataType(idType))
            .collectionName(escapedCollectionName)
            .consistencyLevel(ConsistencyLevel.STRONG)
            .dimension(DEFAULT_DIMENSION);

    CreateCollectionReq.CollectionSchema schema =
        CreateCollectionReq.CollectionSchema.builder().enableDynamicField(true).build();
    schema.addField(
        AddFieldReq.builder()
            .fieldName(MILVUS_PRIMARY_FIELD_NAME)
            .isPrimaryKey(true)
            .dataType(io.milvus.v2.common.DataType.Int64)
            .build());
    schema.addField(
        AddFieldReq.builder()
            .fieldName(MILVUS_VECTOR_FIELD_NAME)
            .dataType(io.milvus.v2.common.DataType.FloatVector)
            .dimension(DEFAULT_DIMENSION)
            .build());
    schema.addField(
        AddFieldReq.builder()
            .fieldName(MILVUS_DATA_FIELD_NAME)
            .dataType(DataTransformer.toMilvusDataType(fieldType))
            .build());

    List<IndexParam> indexes = new ArrayList<>();
    Map<String, Object> extraParams = new HashMap<>();
    extraParams.put("nlist", MILVUS_INDEX_PARAM_NLIST);
    indexes.add(
        IndexParam.builder()
            .fieldName(MILVUS_VECTOR_FIELD_NAME)
            .indexType(DEFAULT_INDEX_TYPE)
            .metricType(DEFAULT_METRIC_TYPE)
            .extraParams(extraParams)
            .build());

    client.createCollection(
        builder
            .collectionSchema(schema)
            .primaryFieldName(MILVUS_PRIMARY_FIELD_NAME)
            .vectorFieldName(MILVUS_VECTOR_FIELD_NAME)
            .build());

    PathUtils.getPathSystem(client, pathSystem)
        .addPath(PathUtils.getPathUnescaped(databaseName, collectionName, ""), false, fieldType);

    client.createIndex(
        CreateIndexReq.builder()
            .collectionName(escapedCollectionName)
            .indexParams(indexes)
            .build());
    client.loadCollection(
        LoadCollectionReq.builder().collectionName(escapedCollectionName).async(false).build());
  }

  public static boolean addProperty(
      JsonObject row, String columnName, Object obj, DataType dataType)
      throws UnsupportedEncodingException {
    columnName = NameUtils.escape(columnName);
    boolean added = false;
    if (obj != null && dataType != null) {
      switch (dataType) {
        case BINARY:
          if (obj instanceof byte[]) {
            row.addProperty(columnName, new String((byte[]) obj, StandardCharsets.UTF_8));
          } else {
            row.addProperty(columnName, (String) obj);
          }
          added = true;
          break;
        case BOOLEAN:
          row.addProperty(columnName, (boolean) obj);
          added = true;
          break;
        case LONG:
        case DOUBLE:
        case INTEGER:
        case FLOAT:
          row.addProperty(columnName, (Number) obj);
          added = true;
          break;
        default:
          break;
      }
    }
    return added;
  }

  public static void dropDatabase(MilvusClientV2 client, String databaseName) {
    try {
      useDatabase(client, databaseName);
    } catch (InterruptedException | UnsupportedEncodingException e) {
      return;
    } catch (IllegalArgumentException e) {
      LOGGER.error("Database " + databaseName + " does not exist.");
      return;
    }
    List<String> collections = client.listCollections().getCollectionNames();
    for (String collectionName : collections) {
      client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }
    client.dropDatabase(DropDatabaseReq.builder().databaseName(databaseName).build());
  }

  public static boolean dropCollection(
      MilvusClientV2 client, String databaseName, String collectionName, Set<String> fields)
      throws UnsupportedEncodingException, InterruptedException {
    client.useDatabase(databaseName);

    if (!client.hasCollection(
        HasCollectionReq.builder().collectionName(NameUtils.escape(collectionName)).build())) {
      return false;
    }

    client.dropCollection(
        DropCollectionReq.builder().collectionName(NameUtils.escape(collectionName)).build());
    return true;
  }

  /**
   * 返回collection名称及与之对应的字段列表 key:collectionName, value:fieldName 未转义,带TagKV
   *
   * @param client
   * @param paths
   * @param tagFilter
   * @return
   */
  public static Map<String, Set<String>> determinePaths(
      MilvusClientV2 client,
      List<String> paths,
      TagFilter tagFilter,
      boolean isDummy,
      PathSystem pathSystem)
      throws UnsupportedEncodingException {
    Set<String> pathSet = new HashSet<>();
    List<String> patterns = paths;
    if (isDummy) {
      patterns = recreateDummyPattern(paths);
    }
    for (String path : patterns) {
      for (String p : PathUtils.getPathSystem(client, pathSystem).findPaths(path, null)) {
        Pair<String, Map<String, String>> columnToTags = splitFullName(getPathAndVersion(p).getK());
        if (tagFilter != null
            && !cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils.match(
                columnToTags.getV(), tagFilter)) {
          continue;
        }
        cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column c =
            PathUtils.getPathSystem(client, pathSystem).getColumn(p);
        if (isDummy && c != null && c.isDummy()) {
          pathSet.add(p);
        } else {
          pathSet.add(p);
        }
      }
    }

    Map<String, Set<String>> collectionToFields = new HashMap<>();
    for (String path : pathSet) {
      if (isDummy) {
        String collectionName = path.substring(0, path.lastIndexOf("."));
        String fieldName = path.substring(path.lastIndexOf(".") + 1);
        collectionToFields.computeIfAbsent(collectionName, k -> new HashSet<>()).add(fieldName);
      } else {
        collectionToFields.computeIfAbsent(path, k -> new HashSet<>()).add(MILVUS_DATA_FIELD_NAME);
      }
    }
    return collectionToFields;
  }

  public static List<String> recreateDummyPattern(List<String> patterns) {
    List<String> newPatterns = new ArrayList<>();
    for (String pattern : patterns) {
      if (pattern.endsWith("*")) {
        newPatterns.add(pattern);
        continue;
      }
      String[] splits = pattern.split("\\.");
      if (splits.length > 2) {
        newPatterns.add(pattern.substring(0, pattern.lastIndexOf(".")) + ".*");
      } else {
        newPatterns.add(pattern);
      }
    }
    return newPatterns;
  }

  public static Map<String, Set<String>> determinePaths(
      MilvusClientV2 client, List<String> paths, TagFilter tagFilter, PathSystem pathSystem)
      throws UnsupportedEncodingException {
    return determinePaths(client, paths, tagFilter, false, pathSystem);
  }

  public static long deleteByRange(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      KeyRange keyRange,
      PathSystem pathSystem)
      throws UnsupportedEncodingException, InterruptedException {
    client.useDatabase(NameUtils.escape(databaseName));
    if (!client.hasCollection(
        HasCollectionReq.builder().collectionName(NameUtils.escape(collectionName)).build())) {
      LOGGER.error(
          "Collection being deleted " + databaseName + " " + collectionName + " does not exist.");
      return 0;
    }
    DeleteResp delete =
        client.delete(
            DeleteReq.builder()
                .collectionName(NameUtils.escape(collectionName))
                .filter(
                    keyRange.getActualBeginKey()
                        + " <= "
                        + MILVUS_PRIMARY_FIELD_NAME
                        + " <= "
                        + keyRange.getActualEndKey())
                .build());

    return delete.getDeleteCnt();
  }

  public static List<Column> query(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      List<String> fields,
      Filter filter,
      List<String> patterns,
      PathSystem pathSystem)
      throws InterruptedException, UnsupportedEncodingException {
    String collectionNameEscaped;
    if (isDummy(databaseName) && !isDummyEscape) {
      collectionNameEscaped = collectionName;
    } else {
      collectionNameEscaped = NameUtils.escape(collectionName);
    }

    useDatabase(client, databaseName);

    try {
      if (!client.getLoadState(
          GetLoadStateReq.builder().collectionName(collectionNameEscaped).build())) {
        client.loadCollection(
            LoadCollectionReq.builder().collectionName(collectionNameEscaped).build());
      }
    } catch (Exception e) {
      LOGGER.error("load collection error {} : {}", databaseName, e.getMessage());
      return new ArrayList<>();
    }

    String primaryFieldName = MILVUS_PRIMARY_FIELD_NAME;
    DescribeCollectionResp describeCollectionResp = null;
    Set<String> deletedFields = new HashSet<>();
    if (isDummy(databaseName)) {
      describeCollectionResp =
          client.describeCollection(
              DescribeCollectionReq.builder().collectionName(collectionNameEscaped).build());

      primaryFieldName = describeCollectionResp.getPrimaryFieldName();
    }

    List<String> fieldsEscaped = escapeList(fields, databaseName);
    if (isDummy(databaseName)
        && describeCollectionResp != null
        && describeCollectionResp.getEnableDynamicField()) {
      fieldsEscaped.add(MILVUS_DYNAMIC_FIELD_NAME);
    }
    List<Pair<Long, Long>> keyRanges = FilterUtils.keyRangesFrom(filter);
    String expr;

    if (isDummy(databaseName) || keyRanges == null || keyRanges.size() < 1) {
      if (describeCollectionResp != null
          && describeCollectionResp.getCollectionSchema().getField(primaryFieldName).getDataType()
              == io.milvus.v2.common.DataType.VarChar) {
        expr = primaryFieldName + ">=''";
      } else {
        expr = primaryFieldName + ">=0";
      }
    } else {
      if (!FilterUtils.filterContainsType(
          Arrays.asList(FilterType.Value, FilterType.Path), filter)) {
        expr =
            new FilterTransformer(primaryFieldName)
                .toString(FilterUtils.expandFilter(filter, primaryFieldName));
      } else {
        expr = primaryFieldName + ">=0";
      }
    }

    Map<String, DataType> pathToDataType = new HashMap<>();
    QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder =
        buildQueryIteratorReq(databaseName, collectionNameEscaped, expr, fieldsEscaped);
    Map<String, Map<Long, Object>> pathToMap =
        iteratorQuery(
            client,
            databaseName,
            collectionName,
            patterns,
            pathSystem,
            primaryFieldName,
            fieldsEscaped,
            deletedFields,
            queryIteratorReqBuilder,
            pathToDataType);

    for (String field : fields) {
      String path = PathUtils.getPathUnescaped(databaseName, collectionName, field);
      if (!pathToMap.containsKey(path)) {
        pathToMap.put(path, new HashMap<>());
      }
    }

    List<Column> columns = new ArrayList<>();
    for (Map.Entry<String, Map<Long, Object>> entry : pathToMap.entrySet()) {
      DataType type = pathToDataType.get(entry.getKey());
      if (type == null) {
        if (PathUtils.getPathSystem(client, pathSystem).getColumn(entry.getKey()) != null) {
          type = pathSystem.getColumn(entry.getKey()).getDataType();
        } else {
          type = DataTransformer.fromObject(entry.getValue().values().iterator().next());
        }
      }

      Column column =
          new Column(
              entry.getKey().replaceAll("\\[\\[[a-zA-Z0-9]+\\]\\]", ""), type, entry.getValue());
      columns.add(column);
    }
    return columns;
  }

  public static List<String> escapeList(List<String> list, String databaseName) {
    if (isDummy(databaseName) && !isDummyEscape) {
      return list;
    } else {
      List<String> result = new ArrayList<>();
      for (String s : list) {
        try {
          result.add(NameUtils.escape(s));
        } catch (UnsupportedEncodingException e) {
        }
      }
      return result;
    }
  }

  private static QueryIteratorReq.QueryIteratorReqBuilder buildQueryIteratorReq(
      String databaseName, String collectionNameEscaped, String expr, List<String> fieldsEscaped) {
    QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder =
        QueryIteratorReq.builder().collectionName(collectionNameEscaped);
    queryIteratorReqBuilder
        .expr(expr)
        .outputFields(fieldsEscaped)
        .consistencyLevel(ConsistencyLevel.STRONG)
        .batchSize(MILVUS_BATCH_SIZE);
    return queryIteratorReqBuilder;
  }

  public static QueryReq.QueryReqBuilder buildQueryReq(
      String databaseName, String collectionNameEscaped, String expr, List<String> fieldsEscaped) {
    QueryReq.QueryReqBuilder queryReqBuilder =
        QueryReq.builder().collectionName(collectionNameEscaped);
    queryReqBuilder.filter(expr);
    if (!isDummy(databaseName)) {
      queryReqBuilder.outputFields(fieldsEscaped);
    }
    queryReqBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    return queryReqBuilder;
  }

  public static long stringToKey(String key) {
    int hashCode = key.hashCode();
    return Integer.toUnsignedLong(hashCode);
  }

  private static Map<String, Map<Long, Object>> iteratorQuery(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      List<String> patterns,
      PathSystem pathSystem,
      String primaryFieldName,
      List<String> fieldsEscaped,
      Set<String> deletedFields,
      QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder,
      Map<String, DataType> pathToDataType)
      throws UnsupportedEncodingException {
    Map<String, Map<Long, Object>> pathToMap = new HashMap<>();
    QueryIterator iterator = client.queryIterator(queryIteratorReqBuilder.build());

    Set<String> matchedKeys = new HashSet<>();
    Set<String> notMatchedKeys = new HashSet<>();

    List<QueryResultsWrapper.RowRecord> list = iterator.next();
    Set<String> fieldSet = new HashSet<>();
    fieldSet.addAll(fieldsEscaped);

    while (list != null && list.size() > 0) {
      long l = System.currentTimeMillis();
      for (QueryResultsWrapper.RowRecord queryResult : list) {
        Map<String, Object> entity = queryResult.getFieldValues();

        for (String key : entity.keySet()) {
          if (key.equals(primaryFieldName)) {
            continue;
          }
          if (deletedFields.contains(NameUtils.unescape(key))) {
            continue;
          }

          String path;
          if (isDummy(databaseName) && !isDummyEscape) {
            path = PathUtils.getPathUnescaped(databaseName, collectionName, key);
          } else {
            path =
                PathUtils.getPathUnescaped(databaseName, collectionName, NameUtils.unescape(key));
          }

          if (isDummy(databaseName) && patterns != null) {
            if (matchedKeys.contains(key)) {
            } else if (notMatchedKeys.contains(key)) {
              continue;
            } else {
              if (PathUtils.match(path, patterns)) {
                matchedKeys.add(key);
              } else {
                notMatchedKeys.add(key);
                continue;
              }
            }
          }

          long keyValue;
          if (entity.get(primaryFieldName) instanceof String) {
            keyValue = stringToKey((String) entity.get(primaryFieldName));
          } else {
            keyValue = (Long) entity.get(primaryFieldName);
          }

          DataType type = pathToDataType.get(path);
          if (type == null) {
            if (PathUtils.getPathSystem(client, pathSystem).getColumn(path) != null) {
              type = pathSystem.getColumn(path).getDataType();
            } else {
              type = DataTransformer.fromObject(entity.get(key));
            }
            pathToDataType.put(path, type);
          }

          pathToMap
              .computeIfAbsent(path, k -> new HashMap<>())
              .put(keyValue, objToDeterminedType(entity.get(key), type));
        }
      }
      if (list.size() < MILVUS_BATCH_SIZE) {
        iterator.close();
        break;
      }
      list = iterator.next();
    }
    return pathToMap;
  }

  public static long upsert(MilvusClientV2 client, String collectionName, List<JsonObject> data)
      throws UnsupportedEncodingException {
    UpsertResp resp =
        client.upsert(
            UpsertReq.builder()
                .collectionName(NameUtils.escape(collectionName))
                .data(data)
                .build());
    return resp.getUpsertCnt();
  }
}
