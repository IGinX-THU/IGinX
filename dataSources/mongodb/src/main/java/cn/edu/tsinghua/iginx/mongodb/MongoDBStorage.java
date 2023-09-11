package cn.edu.tsinghua.iginx.mongodb;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
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
import cn.edu.tsinghua.iginx.mongodb.entity.ColumnQuery;
import cn.edu.tsinghua.iginx.mongodb.entity.DummyQuery;
import cn.edu.tsinghua.iginx.mongodb.entity.JoinQuery;
import cn.edu.tsinghua.iginx.mongodb.entity.SourceTable;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.TypeUtils;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBStorage implements IStorage {

  private static final Logger logger = LoggerFactory.getLogger(MongoDBStorage.class.getName());

  private static final long MAX_WAIT_TIME = 5;
  private static final int SESSION_POOL_MAX_SIZE = 200;
  public static final String VALUE_FIELD = "v";

  private final MongoClient client;

  public MongoDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    if (!meta.getStorageEngine().equals(StorageEngineType.mongodb)) {
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
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(address)))
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxWaitTime(MAX_WAIT_TIME, TimeUnit.SECONDS)
                        .maxSize(SESSION_POOL_MAX_SIZE))
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
      RowStream result = new DummyQuery(this.client).query(patterns, unionFilter);
      return new TaskExecuteResult(result);
    } catch (Exception e) {
      logger.error("dummy project {} where {}", patterns, filter);
      logger.error("failed to dummy query ", e);
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
      }
      return new TaskExecuteResult(result);
    } catch (Exception e) {
      String message = String.format("project %s from %s[%s]", patterns, unit, range);
      if (tagFilter != null) {
        message += " with " + tagFilter;
      }
      logger.error(message, e);
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
      logger.error("delete {} from {} where {} with {}", patterns, unit, ranges, tagFilter);
      logger.error("failed to delete", e);
      return new TaskExecuteResult(new PhysicalException("failed to delete", e));
    }
    return new TaskExecuteResult();
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();
    DataView data = insert.getData();

    try {
      for (SourceTable.Column column : new SourceTable(data)) {
        Field field = column.getField();
        Map<Long, Object> columnData = column.getData();

        List<BsonDocument> documents = new ArrayList<>();
        for (Map.Entry<Long, Object> point : columnData.entrySet()) {
          BsonValue key = new BsonInt64(point.getKey());
          BsonValue value = TypeUtils.toBsonValue(field.getType(), point.getValue());
          BsonDocument document = new BsonDocument("_id", key).append(VALUE_FIELD, value);
          documents.add(document);
        }

        MongoDatabase db = this.getDatabase(unit);
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
      logger.error("failed to insert", e);
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
    for (String unit : this.client.listDatabaseNames()) {
      if (unit.startsWith("unit")) {
        for (Field field : getFields(this.getDatabase(unit))) {
          columns.add(new Column(field.getName(), field.getType(), field.getTags()));
        }
      }
    }
    return columns;
  }

  private MongoDatabase getDatabase(String unit) {
    return this.client.getDatabase(unit);
  }

  private static List<Field> getFields(MongoDatabase db) {
    List<Field> fields = new ArrayList<>();
    for (String collectionName : db.listCollectionNames()) {
      try {
        fields.add(NameUtils.parseCollectionName(collectionName));
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
