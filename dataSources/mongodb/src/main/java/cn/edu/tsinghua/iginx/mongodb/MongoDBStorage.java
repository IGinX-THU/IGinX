package cn.edu.tsinghua.iginx.mongodb;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
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
import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.Query;
import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.ResultTable;
import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.SourceTable;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.TypeUtils;
import cn.edu.tsinghua.iginx.mongodb.local.entity.PathTree;
import cn.edu.tsinghua.iginx.mongodb.local.query.ClientQuery;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBStorage implements IStorage {

  private static final Logger logger = LoggerFactory.getLogger(MongoDBStorage.class.getName());

  private static final String STORAGE_ENGINE = "mongodb";
  private static final long MAX_WAIT_TIME = 5;
  private static final int SESSION_POOL_MAX_SIZE = 200;

  private static final String VALUE_FIELD = "v";

  private final MongoClient client;

  public MongoDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
      throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
    }

    try {
      this.client = connect(meta.getIp(), meta.getPort());
    } catch (Exception e) {
      String message = "fail to connect " + meta.getIp() + ":" + meta.getPort();
      logger.error(message, e);
      throw new StorageInitializationException(message);
    }
  }

  private MongoClient connect(String ip, int port) {
    ServerAddress address = new ServerAddress(ip, port);
    MongoClientSettings settings = MongoClientSettings.builder().applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(address))).applyToConnectionPoolSettings(builder -> builder.maxWaitTime(MAX_WAIT_TIME, TimeUnit.SECONDS).maxSize(SESSION_POOL_MAX_SIZE)).build();

    return MongoClients.create(settings);
  }

  @Override
  public boolean isSupportProjectWithSelect() {
    return false; // TODO
  }

  @Override
  public TaskExecuteResult executeProject(Project project, DataArea area) {
    String unit = area.getStorageUnit();
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();
    TagFilter tagFilter = project.getTagFilter();

    try {
      Collection<Field> fields = this.getMatchedFields(unit, patterns, tagFilter);
      ResultTable.ColumnsBuilder builder = ResultTable.columnsBuilder(fields.size());
      for (Field field : fields) {
        builder.add(field);

        MongoCollection<BsonDocument> collection = this.getCollection(unit, field);
        Bson filter = FilterUtils.interval(range);
        MongoIterable<BsonDocument> result = collection.find(filter);
        for (BsonDocument document : result) {
          long key = document.get("_id").asInt64().getValue();
          BsonValue bsonValue = document.get(VALUE_FIELD);
          Object value = TypeUtils.toObject(bsonValue);
          builder.put(key, value);
        }
      }

      return new TaskExecuteResult(builder.build());
    } catch (Exception e) {
      String message = String.format("project %s from %s[%s]", patterns, unit, range);
      if (tagFilter != null) {
        message += " with " + tagFilter;
      }
      logger.error(message, e);
      return new TaskExecuteResult(new PhysicalException("failed to project", e));
    }
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(Project project, Select select, DataArea area) {
    String unit = area.getStorageUnit();
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();
    TagFilter tags = project.getTagFilter();
    return query(unit, range, patterns, tags, select.getFilter());
  }

  private TaskExecuteResult query(String unit, KeyInterval range, List<String> patterns, TagFilter tags, Filter filter) {
    Query query = new Query();
    try {
      query.range(range);
      if (tags != null) query.filter(tags);
      if (filter != null) query.filter(filter);
      if (patterns != null && !patterns.isEmpty()) query.project(patterns);

//      return new TaskExecuteResult(query.query(getCollection(unit)));
      return new TaskExecuteResult();
    } catch (Exception e) {
      logger.error("project {} from {}[{}] where {} and {}", patterns, unit, range, filter, tags);
      logger.error("failed to query " + query, e);
      return new TaskExecuteResult(new PhysicalException("failed to project", e));
    }
  }

  @Override
  public TaskExecuteResult executeProjectDummy(Project project, DataArea area) {
    return queryDummy(area.getKeyInterval(), project.getPatterns(), null);
  }

  @Override
  public TaskExecuteResult executeProjectDummyWithSelect(Project project, Select select, DataArea area) {
    return queryDummy(area.getKeyInterval(), project.getPatterns(), select.getFilter());
  }

  private TaskExecuteResult queryDummy(KeyInterval range, List<String> patterns, Filter filter) {
    PathTree pathTree = new PathTree();
    if (patterns != null) {
      for (String pattern : patterns) {
        pathTree.put(Arrays.stream(pattern.split("\\.")).iterator());
      }
    }

    AndFilter unionFilter = new AndFilter(new ArrayList<>());
    if (filter != null) unionFilter.getChildren().add(filter);
    if (range != null) {
      KeyFilter leftFilter = new KeyFilter(Op.GE, range.getStartKey());
      KeyFilter rightFilter = new KeyFilter(Op.L, range.getEndKey());
      unionFilter.getChildren().addAll(Arrays.asList(leftFilter, rightFilter));
    }

    try {
      return new TaskExecuteResult(new ClientQuery(this.client).query(pathTree, unionFilter));
    } catch (Exception e) {
      logger.error("dummy project {} where {}", patterns, filter);
      logger.error("failed to dummy query ", e);
      return new TaskExecuteResult(new PhysicalException("failed to query dummy", e));
    }
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();
    List<String> patterns = delete.getPatterns();
    TagFilter tagFilter = delete.getTagFilter();
    List<KeyRange> ranges = delete.getKeyRanges();

    try {
      for (Field field : this.getMatchedFields(unit, patterns, tagFilter)) {
        MongoCollection<BsonDocument> collection = this.getCollection(unit, field);
        if (ranges == null || ranges.isEmpty()) {
          collection.drop();
        } else {
          Bson filter = FilterUtils.ranges(ranges);
          collection.deleteMany(filter);
        }
      }
    } catch (Exception e) {
      logger.error("delete {} from {} where {} with {}", patterns, unit, ranges, tagFilter);
      logger.error("failed to delete", e);
      return new TaskExecuteResult(new PhysicalException("failed to delete", e));
    }
    return new TaskExecuteResult();
  }

  private List<Field> getMatchedFields(String dbName, List<String> patterns, TagFilter tagFilter) {
    List<Field> fields = new ArrayList<>();
    for (Field field : this.getFields(dbName)) {
      if (tagFilter != null && !TagKVUtils.match(field.getTags(), tagFilter)) {
        continue;
      }
      for (String pattern : patterns) {
        if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
          fields.add(field);
          break;
        }
      }
    }
    return fields;
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();

    for (SourceTable.Column column : new SourceTable(insert.getData())) {
      Field field = column.getField();
      Map<Long, Object> data = column.getData();

      List<BsonDocument> documents = new ArrayList<>();
      for (Map.Entry<Long, Object> point : data.entrySet()) {
        BsonValue key = new BsonInt64(point.getKey());
        BsonValue value = TypeUtils.toBsonValue(field.getType(), point.getValue());
        BsonDocument document = new BsonDocument("_id", key).append(VALUE_FIELD, value);
        documents.add(document);
      }

      MongoCollection<BsonDocument> collection = this.getCollection(unit, field);
      try {
        collection.insertMany(documents, new InsertManyOptions().ordered(false));
      } catch (MongoBulkWriteException e) { // has duplicate key, try to upsert
        List<WriteModel<BsonDocument>> writeModels = new ArrayList<>();
        for (WriteError error : e.getWriteErrors()) {
          if (error.getCode() != 11000) { // E11000: duplicate key error
            throw e;
          }

          String msg = error.getMessage();
          String id = msg.substring(msg.lastIndexOf(':') + 2, msg.length() - 2);
          long key = Long.parseLong(id);

          Bson filter = Filters.eq("_id", key);
          BsonValue value = TypeUtils.toBsonValue(field.getType(), data.get(key));
          BsonDocument doc = new BsonDocument(VALUE_FIELD, value);
          ReplaceOptions option = new ReplaceOptions().upsert(true);

          writeModels.add(new ReplaceOneModel<>(filter, doc, option));
        }
        collection.bulkWrite(writeModels, new BulkWriteOptions().ordered(false));
      }
    }

    return new TaskExecuteResult();
  }

  private MongoCollection<BsonDocument> getCollection(String unit, Field field) {
    String collectionName = NameUtils.getFullName(field);
    return this.client.getDatabase(unit).getCollection(collectionName, BsonDocument.class);
  }

  @Override
  public List<Column> getColumns() {
    List<Column> columns = new ArrayList<>();
    for (String dbName : this.client.listDatabaseNames()) {
      if (dbName.startsWith("unit")) {
        for (Field field : this.getFields(dbName)) {
          columns.add(new Column(field.getName(), field.getType(), field.getTags()));
        }
      }
    }
    return columns;
  }

  private List<Field> getFields(String dbName) {
    List<Field> fields = new ArrayList<>();
    for (String collectionName : this.client.getDatabase(dbName).listCollectionNames()) {
      try {
        fields.add(NameUtils.parseFullName(collectionName));
      } catch (Exception e) {
        logger.error("failed to parse collection name: " + collectionName, e);
      }
    }
    return fields;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix) {
    KeyInterval keyInterval = new KeyInterval(0, Long.MAX_VALUE);
    List<String> prefixes = new ArrayList<>();
    for (String db : this.client.listDatabaseNames()) {
      for (String collection : this.client.getDatabase(db).listCollectionNames()) {
        prefixes.add(db + "." + collection);
      }
    }
    ColumnsInterval columnsInterval = new ColumnsInterval("");
    if (!prefixes.isEmpty()) {
      ColumnsInterval first = new ColumnsInterval(Collections.min(prefixes));
      ColumnsInterval last = new ColumnsInterval(Collections.max(prefixes));
      columnsInterval = new ColumnsInterval(first.getStartColumn(), last.getEndColumn());
    }

    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void release() {
    client.close();
  }
}

// TODO dummy 测试一下多个文档，多数据库，多集合的情况
