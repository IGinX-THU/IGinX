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
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBStorage implements IStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBStorage.class);

  private static final long MAX_WAIT_TIME = 5;
  private static final int SESSION_POOL_MAX_SIZE = 200;
  public static final String VALUE_FIELD = "v";
  public static final String[] SYSTEM_DBS = new String[] {"admin", "config", "local"};
  public static final String SCHEMA_SAMPLE_SIZE = "schema.sample.size";
  public static final String QUERY_SAMPLE_SIZE = "dummy.sample.size";
  public static final String SCHEMA_SAMPLE_SIZE_DEFAULT = "1000";
  public static final String QUERY_SAMPLE_SIZE_DEFAULT = "0";

  private final MongoClient client;

  private final int schemaSampleSize;
  private final int querySampleSize;

  public MongoDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.mongodb)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }

    String sampleSize =
        meta.getExtraParams().getOrDefault(SCHEMA_SAMPLE_SIZE, SCHEMA_SAMPLE_SIZE_DEFAULT);
    this.schemaSampleSize = Integer.parseInt(sampleSize);

    String querySampleSize =
        meta.getExtraParams().getOrDefault(QUERY_SAMPLE_SIZE, QUERY_SAMPLE_SIZE_DEFAULT);
    this.querySampleSize = Integer.parseInt(querySampleSize);

    try {
      this.client = connect(meta.getIp(), meta.getPort());
    } catch (Exception e) {
      String message = "fail to connect " + meta.getIp() + ":" + meta.getPort();
      LOGGER.error(message, e);
      throw new StorageInitializationException(message);
    }
  }

  private MongoClient connect(String ip, int port) {
    ServerAddress address = new ServerAddress(ip, port);
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(address)))
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxWaitTime(MAX_WAIT_TIME, TimeUnit.SECONDS)
                        .maxSize(SESSION_POOL_MAX_SIZE)
                        .maxConnectionIdleTime(60, TimeUnit.SECONDS))
            .build();

    return MongoClients.create(settings);
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
    } catch (Exception e) {
      LOGGER.error("dummy project {} where {}", patterns, filter);
      LOGGER.error("failed to dummy query ", e);
      return new TaskExecuteResult(new PhysicalException("failed to query dummy", e));
    }
  }

  private TaskExecuteResult query(Project project, DataArea area, Filter filter) {
    String unit = area.getStorageUnit();
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();
    TagFilter tagFilter = project.getTagFilter();

    try {
      MongoDatabase db = this.getDatabase(unit);
      List<Field> fieldList = NameUtils.match(getFields(db), patterns, tagFilter);

      RowStream result;
      if (filter == null) {
        result = new ColumnQuery(db).query(fieldList, range);
      } else {
        Filter unionFilter = rangeUnionWithFilter(range, filter);
        result = new JoinQuery(db).query(fieldList, unionFilter);
        result = new FilterRowStreamWrapper(result, filter);
      }
      return new TaskExecuteResult(result);
    } catch (Exception e) {
      String message = String.format("project %s from %s[%s]", patterns, unit, range);
      if (tagFilter != null) {
        message += " with " + tagFilter;
      }
      LOGGER.error(message, e);
      return new TaskExecuteResult(new PhysicalException("failed to project", e));
    }
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
    try {
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
    } catch (Exception e) {
      LOGGER.error("delete {} from {} where {} with {}", patterns, unit, ranges, tagFilter);
      LOGGER.error("failed to delete", e);
      return new TaskExecuteResult(new PhysicalException("failed to delete", e));
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
            throw new PhysicalException(
                "data type ("
                    + field.getType()
                    + ") not match existed column type ("
                    + existedType
                    + ")");
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
    } catch (Exception e) {
      LOGGER.error("failed to insert", e);
      return new TaskExecuteResult(new PhysicalException("failed to insert", e));
    }
    return new TaskExecuteResult();
  }

  private static long getDuplicateKey(WriteError error) {
    String msg = error.getMessage();
    String id = msg.substring(msg.lastIndexOf(':') + 2, msg.length() - 2);
    return Long.parseLong(id);
  }

  @Override
  public List<Column> getColumns() {
    List<Column> columns = new ArrayList<>();
    for (String dbName : getDatabaseNames(this.client)) {
      MongoDatabase db = this.client.getDatabase(dbName);
      for (String collectionName : db.listCollectionNames()) {
        try {
          if (dbName.startsWith("unit")) {
            Field field = NameUtils.parseCollectionName(collectionName);
            columns.add(new Column(field.getName(), field.getType(), field.getTags(), false));
            continue;
          }
        } catch (Exception ignored) {
        }

        if (schemaSampleSize > 0) {
          MongoCollection<BsonDocument> collection =
              db.getCollection(collectionName, BsonDocument.class);
          Map<String, DataType> sampleSchema =
              new SchemaSample(schemaSampleSize).query(collection, true);
          for (Map.Entry<String, DataType> entry : sampleSchema.entrySet()) {
            columns.add(new Column(entry.getKey(), entry.getValue(), null, true));
          }
          continue;
        }

        String namespace = dbName + "." + collectionName;
        columns.add(new Column(namespace + ".*", DataType.BINARY, null, true));
      }
    }
    return columns;
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
