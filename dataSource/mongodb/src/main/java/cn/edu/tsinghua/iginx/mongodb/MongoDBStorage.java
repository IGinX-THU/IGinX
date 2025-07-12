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
package cn.edu.tsinghua.iginx.mongodb;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.mongodb.dummy.DummyQuery;
import cn.edu.tsinghua.iginx.mongodb.dummy.SampleQuery;
import cn.edu.tsinghua.iginx.mongodb.dummy.SchemaSample;
import cn.edu.tsinghua.iginx.mongodb.entity.ColumnQuery;
import cn.edu.tsinghua.iginx.mongodb.entity.JoinQuery;
import cn.edu.tsinghua.iginx.mongodb.entity.SourceTable;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.TypeUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBStorage.class);

  private static final long MAX_WAIT_TIME = 5;
  private static final int SESSION_POOL_MAX_SIZE = 200;
  public static final String VALUE_FIELD = "v";
  public static final String[] SYSTEM_DBS = new String[] {"admin", "config", "local"};
  public static final String CONNECTION_STRING = "uri";
  public static final String SCHEMA_SAMPLE_SIZE = "schema.sample.size";
  public static final String QUERY_SAMPLE_SIZE = "dummy.sample.size";
  public static final String SCHEMA_SAMPLE_SIZE_DEFAULT = "1000";
  public static final String QUERY_SAMPLE_SIZE_DEFAULT = "0";

  private final MongoClient client;

  private final int schemaSampleSize;
  private final int querySampleSize;

  private final ExecutorService executor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("MongoDBStorage-%d").build());

  public MongoDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.mongodb)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }

    String defaultConnection = String.format("mongodb://%s:%d", meta.getIp(), meta.getPort());
    String connectionString =
        meta.getExtraParams().getOrDefault(CONNECTION_STRING, defaultConnection);

    String sampleSize =
        meta.getExtraParams().getOrDefault(SCHEMA_SAMPLE_SIZE, SCHEMA_SAMPLE_SIZE_DEFAULT);
    this.schemaSampleSize = Integer.parseInt(sampleSize);

    String querySampleSize =
        meta.getExtraParams().getOrDefault(QUERY_SAMPLE_SIZE, QUERY_SAMPLE_SIZE_DEFAULT);
    this.querySampleSize = Integer.parseInt(querySampleSize);

    try {
      this.client = connect(connectionString);
    } catch (Exception e) {
      String message = "fail to connect " + meta.getIp() + ":" + meta.getPort();
      LOGGER.error(message, e);
      throw new StorageInitializationException(message);
    }
  }

  private MongoClient connect(String connectionString) {
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxWaitTime(MAX_WAIT_TIME, TimeUnit.SECONDS)
                        .maxSize(SESSION_POOL_MAX_SIZE)
                        .maxConnectionIdleTime(60, TimeUnit.SECONDS))
            .applyConnectionString(new ConnectionString(connectionString))
            .build();

    return MongoClients.create(settings);
  }

  @Override
  public boolean testConnection(StorageEngineMeta meta) {
    String defaultConnection = String.format("mongodb://%s:%d", meta.getIp(), meta.getPort());
    String connectionString =
        meta.getExtraParams().getOrDefault(CONNECTION_STRING, defaultConnection);

    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxWaitTime(MAX_WAIT_TIME, TimeUnit.SECONDS)
                        .maxSize(SESSION_POOL_MAX_SIZE)
                        .maxConnectionIdleTime(60, TimeUnit.SECONDS))
            .applyConnectionString(new ConnectionString(connectionString))
            .build();
    try (MongoClient mongoClient = MongoClients.create(settings)) {
      mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to connect MongoDB {}: e", meta, e);
      return false;
    }
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return true;
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea area) {
    return query(project, area, null);
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(Project project, Select select, DataArea area) {
    return query(project, area, select.getFilter());
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea area) {
    return queryDummy(project, area, null);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea area) {
    return queryDummy(project, area, select.getFilter());
  }

  @Override
  public TaskExecuteResult executeProjectWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAggSelect(
      Project project, Select select, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectWithAgg(Project project, Operator agg, DataArea dataArea) {
    return null;
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithAgg(
      Project project, Operator agg, DataArea dataArea) {
    return null;
  }

  private TaskExecuteResult queryDummy(Project project, DataArea area, Filter filter) {
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();

    try {
      Filter unionFilter = rangeUnionWithFilter(range, filter);
      RowStream result;
      if (querySampleSize > 0) {
        result = new SampleQuery(this.client, this.querySampleSize).query(patterns, unionFilter);
      } else {
        result = new DummyQuery(this.client).query(patterns, unionFilter);
      }
      return new TaskExecuteResult(result);
    } catch (PhysicalException | MongoException e) {
      LOGGER.error("dummy project {} where {} failed", patterns, filter, e);
      return new TaskExecuteResult(
          e instanceof PhysicalException ? (PhysicalException) e : new PhysicalException(e));
    }
  }

  private TaskExecuteResult query(Project project, DataArea area, Filter filter) {
    String unit = area.getStorageUnit();
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();
    TagFilter tagFilter = project.getTagFilter();

    MongoDatabase db = this.getDatabase(unit);
    List<Field> fieldList = NameUtils.match(getFields(db), patterns, tagFilter);

    RowStream result;
    try {
      if (filter == null) {
        result = new ColumnQuery(db).query(fieldList, range);
      } else {
        Filter unionFilter = rangeUnionWithFilter(range, filter);
        result = new JoinQuery(db).query(fieldList, unionFilter);
        result = new FilterRowStreamWrapper(result, filter);
      }
    } catch (MongoException e) {
      LOGGER.error(
          "project {} where {} and range [{},{}) failed",
          patterns,
          filter,
          range.getStartKey(),
          range.getEndKey(),
          e);
      return new TaskExecuteResult(new PhysicalException(e));
    }
    return new TaskExecuteResult(result);
  }

  private static Filter rangeUnionWithFilter(KeyInterval range, Filter filter) {
    List<Filter> filters = new ArrayList<>();
    filters.add(new KeyFilter(Op.GE, range.getStartKey()));
    filters.add(new KeyFilter(Op.L, range.getEndKey()));
    if (filter != null) filters.add(filter);
    return new AndFilter(filters);
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();
    List<String> patterns = delete.getPatterns();
    TagFilter tagFilter = delete.getTagFilter();
    List<KeyRange> ranges = delete.getKeyRanges();

    MongoDatabase db = this.getDatabase(unit);
    List<Field> fieldList = NameUtils.match(getFields(db), patterns, tagFilter);
    for (Field field : fieldList) {
      String collName = NameUtils.getCollectionName(field);
      MongoCollection<BsonDocument> coll = db.getCollection(collName, BsonDocument.class);
      if (ranges == null || ranges.isEmpty()) {
        coll.drop();
      } else {
        Bson filter = FilterUtils.ranges(ranges);
        coll.deleteMany(filter);
      }
    }
    return new TaskExecuteResult();
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();
    DataView data = insert.getData();

    try {
      MongoDatabase db = this.getDatabase(unit);
      Map<String, DataType> existedColumnTypes = new HashMap<>();
      for (Field field : getFields(db)) {
        existedColumnTypes.put(field.getName(), field.getType());
      }

      for (SourceTable.Column column : new SourceTable(data)) {
        Field field = column.getField();

        if (existedColumnTypes.containsKey(field.getName())) {
          DataType existedType = existedColumnTypes.get(field.getName());
          if (!existedType.equals(field.getType())) {
            return new TaskExecuteResult(
                new PhysicalException(
                    "data type ("
                        + field.getType()
                        + ") not match existed column type ("
                        + existedType
                        + ")"));
          }
        }

        Map<Long, Object> columnData = column.getData();
        List<BsonDocument> documents = new ArrayList<>();
        for (Map.Entry<Long, Object> point : columnData.entrySet()) {
          BsonValue key = new BsonInt64(point.getKey());
          BsonValue value = TypeUtils.toBsonValue(field.getType(), point.getValue());
          BsonDocument document = new BsonDocument("_id", key).append(VALUE_FIELD, value);
          documents.add(document);
        }

        String collName = NameUtils.getCollectionName(field);
        MongoCollection<BsonDocument> collection = db.getCollection(collName, BsonDocument.class);

        try {
          collection.insertMany(documents, new InsertManyOptions().ordered(false)); // try to insert
        } catch (MongoBulkWriteException e) { // exist duplicate key
          List<WriteModel<BsonDocument>> writeModels = new ArrayList<>();
          for (WriteError error : e.getWriteErrors()) {
            if (error.getCode() != 11000) throw e; // E11000: duplicate key error

            long key = getDuplicateKey(error);
            Bson filter = Filters.eq("_id", key);

            BsonValue value = TypeUtils.toBsonValue(field.getType(), columnData.get(key));
            BsonDocument doc = new BsonDocument(VALUE_FIELD, value);

            writeModels.add(new ReplaceOneModel<>(filter, doc, new ReplaceOptions().upsert(true)));
          }

          collection.bulkWrite(writeModels, new BulkWriteOptions().ordered(false));
        }
      }
    } catch (MongoException e) {
      return new TaskExecuteResult(new PhysicalException(e));
    }
    return new TaskExecuteResult();
  }

  private static long getDuplicateKey(WriteError error) {
    String msg = error.getMessage();
    String id = msg.substring(msg.lastIndexOf(':') + 2, msg.length() - 2);
    return Long.parseLong(id);
  }

  @Override
  public List<Column> getColumns(Set<String> patterns, TagFilter tagFilter) {
    List<String> patternList = new ArrayList<>(patterns);
    if (patternList.isEmpty()) {
      patternList.add("*");
    }

    List<Future<List<Column>>> futures = new ArrayList<>();

    for (String dbName : getDatabaseNames(this.client)) {
      MongoDatabase db = this.client.getDatabase(dbName);
      for (String collectionName : db.listCollectionNames()) {
        Future<List<Column>> future =
            this.executor.submit(
                () -> {
                  List<Column> columns = new ArrayList<>();
                  if (dbName.startsWith("unit")) {
                    try {
                      Field field = NameUtils.parseCollectionName(collectionName);
                      if (NameUtils.match(
                          field.getName(), field.getTags(), patternList, tagFilter)) {
                        columns.add(
                            new Column(field.getName(), field.getType(), field.getTags(), false));
                      }
                    } catch (Exception ignored) {
                    }
                  } else if (schemaSampleSize > 0) {
                    MongoCollection<BsonDocument> collection =
                        db.getCollection(collectionName, BsonDocument.class);
                    Map<String, DataType> sampleSchema =
                        new SchemaSample(schemaSampleSize).query(collection, true);
                    for (Map.Entry<String, DataType> entry : sampleSchema.entrySet()) {
                      if (NameUtils.match(
                          entry.getKey(), Collections.emptyMap(), patternList, null)) {
                        columns.add(new Column(entry.getKey(), entry.getValue(), null, true));
                      }
                    }
                  } else {
                    String namespace = dbName + "." + collectionName;
                    columns.add(new Column(namespace + ".*", DataType.BINARY, null, true));
                  }
                  return columns;
                });
        futures.add(future);
      }
    }

    List<Column> targetColumns = new ArrayList<>();
    try {
      for (Future<List<Column>> future : futures) {
        targetColumns.addAll(future.get());
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
    return targetColumns;
  }

  public static List<String> getDatabaseNames(MongoClient client) {
    List<String> names = new ArrayList<>();
    for (String dbName : client.listDatabaseNames()) {
      if (Arrays.stream(SYSTEM_DBS).noneMatch(name -> name.equals(dbName))) {
        names.add(dbName);
      }
    }
    return names;
  }

  private MongoDatabase getDatabase(String dbName) {
    return this.client.getDatabase(dbName);
  }

  private static List<Field> getFields(MongoDatabase db) {
    List<Field> fields = new ArrayList<>();
    for (String collectionName : db.listCollectionNames()) {
      try {
        fields.add(NameUtils.parseCollectionName(collectionName));
      } catch (ParseException e) {
        throw new IllegalStateException("failed to parse collection name: " + collectionName, e);
      }
    }
    return fields;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    if (prefix == null) {
      prefix = "";
    }

    String namespacePrefix =
        Arrays.stream(prefix.split("\\.")).limit(2).collect(Collectors.joining("."));

    List<String> namespaces = new ArrayList<>();
    for (String db : getDatabaseNames(this.client)) {
      for (String collection : this.client.getDatabase(db).listCollectionNames()) {
        String namespace = db + "." + collection;
        if (namespace.startsWith(namespacePrefix)) {
          namespaces.add(namespace);
        }
      }
    }

    if (namespaces.isEmpty()) {
      throw new PhysicalTaskExecuteFailureException("no data!");
    }
    ColumnsInterval first = new ColumnsInterval(Collections.min(namespaces));
    ColumnsInterval last = new ColumnsInterval(Collections.max(namespaces));
    ColumnsInterval columnsInterval =
        new ColumnsInterval(first.getStartColumn(), last.getEndColumn());

    KeyInterval keyInterval = KeyInterval.getDefaultKeyInterval();
    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void release() {
    client.close();
  }
}
