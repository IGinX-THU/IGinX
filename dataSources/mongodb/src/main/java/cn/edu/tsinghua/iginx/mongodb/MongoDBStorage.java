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
import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.MongoRow;
import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.MongoTable;
import cn.edu.tsinghua.iginx.mongodb.immigrant.entity.Query;
import cn.edu.tsinghua.iginx.mongodb.immigrant.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.immigrant.tools.UpdateUtils;
import cn.edu.tsinghua.iginx.mongodb.local.LocalStorage;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBStorage implements IStorage {

  private static final Logger logger = LoggerFactory.getLogger(MongoDBStorage.class.getName());

  private static final String STORAGE_ENGINE = "mongodb";
  private static final long MAX_WAIT_TIME = 5;
  private static final int SESSION_POOL_MAX_SIZE = 200;

  private static final String DEFAULT_COLLECTION_NAME = "default";

  private final MongoClient client;

  private final LocalStorage localStorage;

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

    this.localStorage = new LocalStorage(client, logger);
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
    String unit = area.getStorageUnit();
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();
    TagFilter tags = project.getTagFilter();
    return query(unit, range, patterns, tags, null);
  }

  @Override
  public TaskExecuteResult executeProjectWithSelect(Project project, Select select, DataArea area) {
    String unit = area.getStorageUnit();
    KeyInterval range = area.getKeyInterval();
    List<String> patterns = project.getPatterns();
    TagFilter tags = project.getTagFilter();
    return query(unit, range, patterns, tags, select.getFilter());
  }

  private TaskExecuteResult query(
      String unit, KeyInterval range, List<String> patterns, TagFilter tags, Filter filter) {
    Query query = new Query();
    try {
      query.range(range);
      if (tags != null) query.filter(tags);
      if (filter != null) query.filter(filter);
      if (patterns != null && !patterns.isEmpty()) query.project(patterns);

      return new TaskExecuteResult(query.query(getCollection(unit)));
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
  public TaskExecuteResult executeProjectDummyWithSelect(
      Project project, Select select, DataArea area) {
    return queryDummy(area.getKeyInterval(), project.getPatterns(), select.getFilter());
  }

  private TaskExecuteResult queryDummy(KeyInterval range, List<String> patterns, Filter filter) {
    try {
      if (patterns == null) patterns = new ArrayList<>();

      AndFilter unionFilter = new AndFilter(new ArrayList<>());
      if (filter != null) unionFilter.getChildren().add(filter);
      if (range != null) {
        KeyFilter leftFilter = new KeyFilter(Op.GE, range.getStartKey());
        KeyFilter rightFilter = new KeyFilter(Op.L, range.getEndKey());
        unionFilter.getChildren().addAll(Arrays.asList(leftFilter, rightFilter));
      }

      return new TaskExecuteResult(localStorage.query(patterns, unionFilter));
    } catch (Exception e) {
      return new TaskExecuteResult(new PhysicalException("failed to query dummy", e));
    }
  }

  @Override
  public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();
    List<String> paths = delete.getPatterns();
    List<KeyRange> ranges = delete.getKeyRanges();
    TagFilter tags = delete.getTagFilter();

    List<WriteModel<BsonDocument>> operations = new ArrayList<>();
    try {
      Bson filter = new BsonDocument();
      if ((ranges != null && !ranges.isEmpty()) || tags != null) {
        if (tags == null) {
          filter = FilterUtils.ranges(ranges);
        } else if (ranges == null || ranges.isEmpty()) {
          filter = FilterUtils.withTags(tags);
        } else {
          filter = Filters.and(FilterUtils.withTags(tags), FilterUtils.ranges(ranges));
        }
      }

      if (!paths.isEmpty()) {
        if (paths.stream().anyMatch(NameUtils::isWildcardAll)) { // match all columns
          operations.add(new DeleteManyModel<>(filter));
        } else {
          if (paths.stream().anyMatch(NameUtils::isWildcard)) { // match the wildcard
            operations.add(new UpdateManyModel<>(filter, UpdateUtils.wildcardUnset(paths)));
          } else {
            operations.add(new UpdateManyModel<>(filter, UpdateUtils.unset(paths)));
          }
          operations.add(new DeleteManyModel<>(FilterUtils.nonFieldExcept("$$ROOT", "_id")));
        }
        this.getCollection(unit).bulkWrite(operations);
      }
      return new TaskExecuteResult();
    } catch (Exception e) {
      logger.error("delete {} from {} where {} and {}", paths, unit, ranges, tags);
      logger.error("failed to write " + operations, e);
      return new TaskExecuteResult(new PhysicalException("failed to delete", e));
    }
  }

  @Override
  public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
    String unit = dataArea.getStorageUnit();
    DataView data = insert.getData();

    List<WriteModel<BsonDocument>> operations = new ArrayList<>();
    try {
      Consumer<MongoRow> toUpsert =
          row -> {
            Bson filter = new BsonDocument("_id", row.getBsonId());
            Bson update = new BsonDocument("$set", row.getBsonFields());
            operations.add(new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true)));
          };

      MongoTable table = new MongoTable(data);
      for (MongoRow row : table) toUpsert.accept(row);
      for (MongoRow row : table.headers()) toUpsert.accept(row); // upsert place holder

      this.getCollection(unit).bulkWrite(operations, new BulkWriteOptions().ordered(false));
      return new TaskExecuteResult();
    } catch (Exception e) {
      logger.error("insert into {} values {}*{}", unit, data.getKeySize(), data.getPaths());
      logger.error("failed to write " + operations, e);
      return new TaskExecuteResult(new PhysicalException("failed to insert", e));
    }
  }

  @Override
  public List<Column> getColumns() {
    Set<Column> columnSet = new HashSet<>();
    try {
      for (MongoCollection<BsonDocument> mongoCollection : getCollections()) {
        RowStream result = new Query().last().query(mongoCollection);
        for (Field field : result.getHeader().getFields()) {
          columnSet.add(new Column(field.getName(), field.getType(), field.getTags()));
        }
      }
    } catch (Exception e) {
      logger.error("failed to get columns", e);
      return null;
    }
    return new ArrayList<>(columnSet);
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

  private MongoCollection<BsonDocument> getCollection(String unit) {
    String dbName = "iginx_" + unit;
    return this.client
        .getDatabase(dbName)
        .getCollection(DEFAULT_COLLECTION_NAME, BsonDocument.class);
  }

  private Iterable<MongoCollection<BsonDocument>> getCollections() {
    return this.client
        .listDatabaseNames()
        .map(
            dbName ->
                this.client
                    .getDatabase(dbName)
                    .getCollection(DEFAULT_COLLECTION_NAME, BsonDocument.class));
  }

  @Override
  public void release() {
    client.close();
  }
}
