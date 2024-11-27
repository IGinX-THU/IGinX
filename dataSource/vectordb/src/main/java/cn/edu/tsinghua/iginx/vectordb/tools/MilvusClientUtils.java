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

import static cn.edu.tsinghua.iginx.vectordb.MilvusStorage.BATCH_SIZE;
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
import com.google.gson.reflect.TypeToken;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.UpsertResp;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
          //            result.add(NameUtils.unescape(s));
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
      MilvusClientV2 client, String dbName, String collection) throws UnsupportedEncodingException {
    final String databaseName;
    final String collectionName;
    if (isDummy(dbName) && !isDummyEscape) {
      databaseName = dbName;
      collectionName = collection;
    } else {
      databaseName = NameUtils.escape(dbName);
      collectionName = NameUtils.escape(collection);
    }
    DescribeCollectionResp resp =
        client.describeCollection(
            DescribeCollectionReq.builder().collectionName(collectionName).build());
    Map<String, DataType> paths = new HashMap<>();
    resp.getCollectionSchema()
        .getFieldSchemaList()
        .forEach(
            fieldSchema -> {
              if (!fieldSchema.getIsPrimaryKey()
                  && !fieldSchema.getName().equals(MILVUS_VECTOR_FIELD_NAME)) {
                paths.put(
                    PathUtils.getPathEscaped(databaseName, collectionName, fieldSchema.getName()),
                    DataTransformer.fromMilvusDataType(fieldSchema.getDataType()));
              }
            });

    resp.getProperties()
        .forEach(
            (k, v) -> {
              if (k.startsWith(DYNAMIC_FIELDS_PROPERTIES_PREFIX)) {
                String fieldName = k.substring(DYNAMIC_FIELDS_PROPERTIES_PREFIX.length());
                Map<String, String> map =
                    JsonUtils.jsonToType(v, new TypeToken<Map<String, String>>() {});
                if (map.containsKey(Constants.KEY_PROPERTY_DELETED)) {
                  paths.remove(PathUtils.getPathEscaped(databaseName, collectionName, fieldName));
                } else if (map.containsKey(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME)) {
                  paths.remove(PathUtils.getPathEscaped(databaseName, collectionName, fieldName));
                  paths.put(
                      PathUtils.getPathEscaped(
                          databaseName,
                          collectionName,
                          map.get(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME)),
                      DataTransformer.fromStringDataType(
                          map.get(Constants.KEY_PROPERTY_FIELD_DATA_TYPE)));
                }
              }
            });

    return paths;
  }

  public static void createDatabase(MilvusClientV2 client, String databaseName) {
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
    }
  }

  public static Map<String, String> getCollectionFields(
      MilvusClientV2 client, String collectionName) {
    DescribeCollectionResp resp =
        client.describeCollection(
            DescribeCollectionReq.builder().collectionName(collectionName).build());
    Map<String, String> fields = new HashMap<>();
    resp.getCollectionSchema()
        .getFieldSchemaList()
        .forEach(
            fieldSchema -> {
              fields.put(fieldSchema.getName(), fieldSchema.getName());
            });

    resp.getProperties()
        .forEach(
            (k, v) -> {
              if (k.startsWith(DYNAMIC_FIELDS_PROPERTIES_PREFIX)) {
                String fieldName = k.substring(DYNAMIC_FIELDS_PROPERTIES_PREFIX.length());
                Map<String, String> map =
                    JsonUtils.jsonToType(v, new TypeToken<Map<String, String>>() {});
                if (map.containsKey(Constants.KEY_PROPERTY_DELETED)) {
                  fields.remove(fieldName);
                } else if (map.containsKey(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME)) {
                  fields.remove(fieldName);
                  fields.put(fieldName, map.get(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME));
                }
              }
            });

    return fields;
  }

  public static void createCollection(
      MilvusClientV2 client, String databaseName, String collectionName, DataType idType)
      throws InterruptedException, UnsupportedEncodingException {
    createCollection(client, databaseName, collectionName, idType, null, null);
  }

  public static void createCollection(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      DataType idType,
      Set<String> fieldsToAdd,
      Map<String, DataType> fieldTypes)
      throws InterruptedException, UnsupportedEncodingException {
    useDatabase(client, databaseName);
    CreateCollectionReq.CreateCollectionReqBuilder builder =
        CreateCollectionReq.builder()
            .idType(DataTransformer.toMilvusDataType(idType))
            .collectionName(NameUtils.escape(collectionName))
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
            .fieldName(MILVUS_VECTOR_FIELD_NAME)
            .indexName(MILVUS_VECTOR_INDEX_NAME)
            .indexType(DEFAULT_INDEX_TYPE)
            .metricType(DEFAULT_METRIC_TYPE)
            .extraParams(extraParams)
            .build());

    client.createCollection(
        builder
            .collectionSchema(schema)
            .primaryFieldName(MILVUS_PRIMARY_FIELD_NAME)
            .vectorFieldName(MILVUS_VECTOR_FIELD_NAME)
            .indexParams(indexes)
            .build());
  }

  /**
   * 添加新增字段，返回新增字段完整名称（未转义）
   *
   * @param client
   * @param collectionName
   * @param fieldsToAdd with tags
   * @param fieldTypes
   * @return
   */
  public static Map<String, String> addCollectionFields(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      Set<String> fieldsToAdd,
      Map<String, DataType> fieldTypes,
      PathSystem pathSystem)
      throws UnsupportedEncodingException {
    DescribeCollectionResp resp =
        client.describeCollection(
            DescribeCollectionReq.builder()
                .collectionName(NameUtils.escape(collectionName))
                .build());
    Map<String, String> fields = new HashMap<>();
    Map<String, String> fieldsDeleted = new HashMap<>();
    resp.getCollectionSchema()
        .getFieldSchemaList()
        .forEach(
            fieldSchema -> {
              fields.put(fieldSchema.getName(), fieldSchema.getName());
            });
    resp.getProperties()
        .forEach(
            (k, v) -> {
              if (k.startsWith(DYNAMIC_FIELDS_PROPERTIES_PREFIX)) {
                String fieldName = k.substring(DYNAMIC_FIELDS_PROPERTIES_PREFIX.length());
                Map<String, String> map =
                    JsonUtils.jsonToType(v, new TypeToken<Map<String, String>>() {});
                if (map.containsKey(Constants.KEY_PROPERTY_DELETED)) {
                  fields.remove(fieldName);
                  fieldsDeleted.put(fieldName, map.get(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME));
                } else if (map.containsKey(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME)) {
                  fields.remove(fieldName);
                  fields.put(fieldName, map.get(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME));
                }
              }
            });

    Map<String, String> result = new HashMap<>();
    AlterCollectionReq.AlterCollectionReqBuilder alterCollectionReqBuilder =
        AlterCollectionReq.builder()
            .databaseName(NameUtils.escape(databaseName))
            .collectionName(NameUtils.escape(collectionName));
    boolean added = false;
    for (String f : fieldsToAdd) {
      if (fields.containsKey(f)) {
        // 已存在，直接取出
        result.put(f, fields.get(f));
      } else if (fieldsDeleted.containsKey(f)) {
        // 已删除，重新生成，并记录到properties里
        String deletedFieldName = fieldsDeleted.get(f);
        Pair<String, Integer> fieldNameAndVersion = NameUtils.getPathAndVersion(deletedFieldName);
        String newFieldName =
            new StringBuilder(fieldNameAndVersion.getK())
                .append("[[")
                .append(fieldNameAndVersion.getV() + 1)
                .append("]]")
                .toString();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME, newFieldName);
        map.put(Constants.KEY_PROPERTY_FIELD_DATA_TYPE, fieldTypes.get(f).name());
        alterCollectionReqBuilder.property(
            DYNAMIC_FIELDS_PROPERTIES_PREFIX + f, JsonUtils.toJson(map));
        PathUtils.getPathSystem(client, pathSystem)
            .addPath(
                PathUtils.getPathUnescaped(databaseName, collectionName, newFieldName),
                false,
                fieldTypes.get(f));
        result.put(f, newFieldName);
        added = true;
      } else {
        // 直接添加
        result.put(f, f);
        Map<String, String> map = new HashMap<>();
        map.put(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME, f);
        map.put(Constants.KEY_PROPERTY_FIELD_DATA_TYPE, fieldTypes.get(f).name());
        alterCollectionReqBuilder.property(
            DYNAMIC_FIELDS_PROPERTIES_PREFIX + f, JsonUtils.toJson(map));
        PathUtils.getPathSystem(client, pathSystem)
            .addPath(
                PathUtils.getPathUnescaped(databaseName, collectionName, f),
                false,
                fieldTypes.get(f));
        added = true;
      }
    }
    if (added) {
      client.alterCollection(alterCollectionReqBuilder.build());
    }

    return result;
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

  public static JsonObject updateRow(
      Map<String, Object> record,
      JsonObject row,
      Set<String> fields,
      Map<String, DataType> fieldToDataType)
      throws UnsupportedEncodingException {
    Long id = row.get(MILVUS_PRIMARY_FIELD_NAME).getAsLong();
    if (!id.equals(record.get(MILVUS_PRIMARY_FIELD_NAME))) {
      return row;
    }
    for (Map.Entry<String, Object> entry : record.entrySet()) {
      String fieldName = NameUtils.unescape(entry.getKey());
      if (!fields.contains(fieldName)) {
        Object obj = entry.getValue();
        if (obj != null) {
          MilvusClientUtils.addProperty(row, fieldName, obj, fieldToDataType.get(fieldName));
        }
      }
    }
    return row;
  }

  public static long upsert(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      List<JsonObject> data,
      List<Object> ids,
      Set<String> fields,
      PathSystem pathSystem)
      throws InterruptedException, UnsupportedEncodingException {
    List<String> paths =
        PathUtils.getPathSystem(client, pathSystem).findPaths(collectionName, null);

    Set<String> fieldList = new HashSet<>();
    Map<String, DataType> fieldToDataType = new HashMap<>();
    for (String path : paths) {
      fieldList.add(NameUtils.escape(path.substring(path.lastIndexOf(".") + 1)));
      fieldToDataType.put(
          path.substring(path.lastIndexOf(".") + 1),
          PathUtils.getPathSystem(client, pathSystem).getColumn(path).getDataType());
    }
    fieldList.add(MILVUS_VECTOR_FIELD_NAME);

    useDatabase(client, databaseName);

    GetReq getReq =
        GetReq.builder()
            .collectionName(NameUtils.escape(collectionName))
            .ids(ids)
            .outputFields(new ArrayList<>(fieldList))
            .build();

    Map<Long, Map<String, Object>> records = new HashMap<>();
    client
        .get(getReq)
        .getGetResults()
        .forEach(
            record -> {
              records.put(
                  (Long) record.getEntity().get(MILVUS_PRIMARY_FIELD_NAME), record.getEntity());
            });

    for (JsonObject row : data) {
      Long id = row.get(MILVUS_PRIMARY_FIELD_NAME).getAsLong();
      if (records.containsKey(id)) {
        updateRow(records.get(id), row, fields, fieldToDataType);
      }
    }

    UpsertResp resp =
        client.upsert(
            UpsertReq.builder()
                .collectionName(NameUtils.escape(collectionName))
                .data(data)
                .build());
    return resp.getUpsertCnt();
  }

  public static void dropDatabase(MilvusClientV2 client, String databaseName) {
    try {
      useDatabase(client, databaseName);
    } catch (InterruptedException | UnsupportedEncodingException e) {
      return;
    } catch (IllegalArgumentException e) {
      LOGGER.info("Database " + databaseName + " does not exist.");
      return;
    }
    List<String> collections = client.listCollections().getCollectionNames();
    for (String collectionName : collections) {
      client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }
    client.dropDatabase(DropDatabaseReq.builder().databaseName(databaseName).build());
  }

  public static void dropFields(MilvusClientV2 client, String collectionName, Set<String> fields)
      throws UnsupportedEncodingException {
    if (!client.hasCollection(
        HasCollectionReq.builder().collectionName(NameUtils.escape(collectionName)).build())) {
      return;
    }

    AlterCollectionReq.AlterCollectionReqBuilder alterCollectionReqBuilder =
        AlterCollectionReq.builder().collectionName(NameUtils.escape(collectionName));
    for (String field : fields) {
      Map<String, String> map = new HashMap<>();
      Pair<String, Integer> fieldNameAndVersion = NameUtils.getPathAndVersion(field);
      map.put(Constants.KEY_PROPERTY_CURRENT_FIELD_NAME, field);
      map.put(KEY_PROPERTY_DELETED, field);
      alterCollectionReqBuilder.property(
          DYNAMIC_FIELDS_PROPERTIES_PREFIX + fieldNameAndVersion.getK(), JsonUtils.toJson(map));
    }
    client.alterCollection(alterCollectionReqBuilder.build());
  }

  /**
   * 返回collection名称及与之对应的字段列表 key:collectionName, value:fieldName 未转义,带TagKV及版本号
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
      String collectionName = path.substring(0, path.lastIndexOf("."));
      String fieldName = path.substring(path.lastIndexOf(".") + 1);
      collectionToFields.computeIfAbsent(collectionName, k -> new HashSet<>()).add(fieldName);
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

  public static int deleteFieldsByRange(
      MilvusClientV2 client,
      String collectionName,
      Set<String> fields,
      KeyRange keyRange,
      PathSystem pathSystem)
      throws UnsupportedEncodingException {
    if (!client.hasCollection(
        HasCollectionReq.builder().collectionName(NameUtils.escape(collectionName)).build())) {
      return 0;
    }
    int deleteCount = 0;

    List<String> paths =
        PathUtils.getPathSystem(client, pathSystem).findPaths(collectionName, null);
    if (paths == null || paths.size() < 1) {
      return 0;
    }

    Set<String> fieldList = new HashSet<>();
    for (String path : paths) {
      fieldList.add(NameUtils.escape(path.substring(path.lastIndexOf(".") + 1)));
    }
    fieldList.add(MILVUS_VECTOR_FIELD_NAME);
    //    fieldList.add(MILVUS_DYNAMIC_FIELD_NAME);

    QueryIterator iterator =
        client.queryIterator(
            QueryIteratorReq.builder()
                .collectionName(NameUtils.escape(collectionName))
                .expr(
                    keyRange.getActualBeginKey()
                        + " <= "
                        + MILVUS_PRIMARY_FIELD_NAME
                        + " <= "
                        + keyRange.getActualEndKey())
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(new ArrayList<>(fieldList))
                .batchSize(BATCH_SIZE)
                .build());

    List<QueryResultsWrapper.RowRecord> list = iterator.next();
    while (list != null && list.size() > 0) {
      List<JsonObject> data = new ArrayList<>();
      list.forEach(
          record -> {
            Map<String, Object> map = record.getFieldValues();
            for (String field : fields) {
              try {
                map.remove(NameUtils.escape(field));
              } catch (UnsupportedEncodingException e) {
              }
            }
            data.add(JsonUtils.mapToJson(map));
          });
      UpsertResp upsertResp =
          client.upsert(
              UpsertReq.builder()
                  .collectionName(NameUtils.escape(collectionName))
                  .data(data)
                  .build());
      deleteCount += upsertResp.getUpsertCnt();
      list = iterator.next();
    }
    return deleteCount;
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

    if (!client.hasCollection(
        HasCollectionReq.builder().collectionName(collectionNameEscaped).build())) {
      LOGGER.error("collection not exists {} : {}", databaseName, collectionName);
      return new ArrayList<>();
    }

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

    DescribeCollectionResp describeCollectionResp =
        client.describeCollection(
            DescribeCollectionReq.builder().collectionName(collectionNameEscaped).build());

    String primaryFieldName = describeCollectionResp.getPrimaryFieldName();
    Set<String> deletedFields = new HashSet<>();
    for (Map.Entry<String, String> entry : describeCollectionResp.getProperties().entrySet()) {
      if (entry.getKey().startsWith(DYNAMIC_FIELDS_PROPERTIES_PREFIX)) {
        try {
          String fieldName = entry.getKey().substring(DYNAMIC_FIELDS_PROPERTIES_PREFIX.length());
          Map<String, String> map =
              JsonUtils.jsonToType(entry.getValue(), new TypeToken<Map<String, String>>() {});
          if (map.containsKey(Constants.KEY_PROPERTY_DELETED)) {
            deletedFields.add(fieldName);
            pathSystem.deletePath(
                PathUtils.getPathUnescaped(databaseName, collectionName, fieldName));
            fields.remove(fieldName);
          }
        } catch (Exception e) {
        }
      }
    }

    List<String> fieldsEscaped = escapeList(fields, databaseName);
    if (isDummy(databaseName) && describeCollectionResp.getEnableDynamicField()) {
      fieldsEscaped.add(MILVUS_DYNAMIC_FIELD_NAME);
    }
    List<Pair<Long, Long>> keyRanges = FilterUtils.keyRangesFrom(filter);
    String expr;

    if (isDummy(databaseName) || keyRanges == null || keyRanges.size() < 1) {
      expr = primaryFieldName + ">=0";
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
            queryIteratorReqBuilder);

    for (String field : fields) {
      String path = PathUtils.getPathUnescaped(databaseName, collectionName, field);
      if (!pathToMap.containsKey(path)) {
        pathToMap.put(path, new HashMap<>());
      }
    }

    Map<String, DataType> pathToDataType = new HashMap<>();
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
          new Column(entry.getKey().replaceAll("\\[\\[(\\d+)\\]\\]", ""), type, entry.getValue());

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

  public static QueryIteratorReq.QueryIteratorReqBuilder buildQueryIteratorReq(
      String databaseName, String collectionNameEscaped, String expr, List<String> fieldsEscaped) {
    QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder =
        QueryIteratorReq.builder().collectionName(collectionNameEscaped);
    queryIteratorReqBuilder.expr(expr);
    queryIteratorReqBuilder.outputFields(fieldsEscaped);
    queryIteratorReqBuilder.consistencyLevel(ConsistencyLevel.STRONG);
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

  private static Map<String, Map<Long, Object>> iteratorQuery(
      MilvusClientV2 client,
      String databaseName,
      String collectionName,
      List<String> patterns,
      PathSystem pathSystem,
      String primaryFieldName,
      List<String> fieldsEscaped,
      Set<String> deletedFields,
      QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder) {
    Map<String, Map<Long, Object>> pathToMap = new HashMap<>();
    QueryIterator iterator = client.queryIterator(queryIteratorReqBuilder.build());

    Set<String> matchedKeys = new HashSet<>();
    Set<String> notMatchedKeys = new HashSet<>();

    List<QueryResultsWrapper.RowRecord> list = iterator.next();
    Set<String> fieldSet = new HashSet<>();
    fieldSet.addAll(fieldsEscaped);
    while (list != null && list.size() > 0) {
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

          pathToMap
              .computeIfAbsent(path, k -> new HashMap<>())
              .put(
                  (Long) entity.get(primaryFieldName),
                  objToDeterminedType(
                      entity.get(key),
                      pathSystem.getColumn(path) != null
                          ? pathSystem.getColumn(path).getDataType()
                          : DataTransformer.fromObject(entity.get(key))));
        }
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
