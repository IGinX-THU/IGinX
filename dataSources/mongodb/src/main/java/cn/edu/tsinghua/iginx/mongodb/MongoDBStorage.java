package cn.edu.tsinghua.iginx.mongodb;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.mongodb.query.entity.MongoDBQueryRowStream;
import cn.edu.tsinghua.iginx.mongodb.tools.DataUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.*;

// due to its schema, mongodb doesn't support history data
public class MongoDBStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBStorage.class.getName());

    private static final String STORAGE_ENGINE = "mongodb";

    private static final String CONNECTION_STRING = "mongodb://%s:%d";

    private static final String DATABASE = "IGinX";

    public static final String NAME = "name";

    public static final String TAG_PREFIX = "tag_";

    public static final String VALUES = "values";

    public static final String TYPE = "type";

    private final StorageEngineMeta meta;

    private final int SESSION_POOL_MAX_SIZE = 200;

    private final int MAX_WAIT_TIME = 5;

    private MongoClient mongoClient;

    private MongoDatabase mongoDatabase;

    private ConcurrentMap<String, MongoCollection<Document>> collectionMap = new ConcurrentHashMap<>();


    public MongoDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        init();
    }

    private void init() {
        String connectionString = String.format(CONNECTION_STRING, meta.getIp(), meta.getPort());
        mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString))
            .applyToConnectionPoolSettings(builder ->
                builder.maxWaitTime(MAX_WAIT_TIME, TimeUnit.SECONDS)
                    .maxSize(SESSION_POOL_MAX_SIZE)).build());
        mongoDatabase = mongoClient.getDatabase(DATABASE);
    }

    private MongoCollection<Document> getCollection(String id) {
        return collectionMap.computeIfAbsent(id, name -> {
            try {
                return mongoDatabase.getCollection(id);
            } catch (Exception e) {
                logger.error("init collection error: ", e);
                return null;
            }
        });
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();
        if (op.getType() == OperatorType.Project) { // 目前只实现 project 操作符，同时不支持历史数据
            Project project = (Project) op;
            return executeProjectTask(fragment.getTimeInterval(), storageUnit, project);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(storageUnit, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executeDeleteTask(storageUnit, delete);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    private TaskExecuteResult executeInsertTask(String storageUnit, Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertRowRecords((RowDataView) dataView, storageUnit);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in influxdb failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private Bson genPatternBson(List<String> patterns) {
        List<Bson> patternRegexes = new ArrayList<>();
        for (String pattern : patterns) {
            patternRegexes.add(regex(NAME, DataUtils.reformatPattern(pattern)));
        }
        return or(patternRegexes);
    }

    private Bson genTagKVBson(TagFilter tagFilter) {
        switch (tagFilter.getType()) {
            case Base:
                BaseTagFilter baseTagFilter = (BaseTagFilter) tagFilter;
                return eq(TAG_PREFIX + baseTagFilter.getTagKey(), baseTagFilter.getTagValue());
            case And:
                AndTagFilter andTagFilter = (AndTagFilter) tagFilter;
                List<Bson> andBsonFilters = new ArrayList<>();
                for (TagFilter subTagFilter : andTagFilter.getChildren()) {
                    andBsonFilters.add(genTagKVBson(subTagFilter));
                }
                return and(andBsonFilters);
            case Or:
                OrTagFilter orTagFilter = (OrTagFilter) tagFilter;
                List<Bson> orBsonFilters = new ArrayList<>();
                for (TagFilter subTagFilter : orTagFilter.getChildren()) {
                    orBsonFilters.add(genTagKVBson(subTagFilter));
                }
                return or(orBsonFilters);
            case BasePrecise:
                BasePreciseTagFilter basePreciseTagFilter = (BasePreciseTagFilter) tagFilter;
                List<Bson> basePreciseBsonFilters = new ArrayList<>();
                Map<String, String> basePreciseMap = basePreciseTagFilter.getTags();
                for (Map.Entry<String, String> basePreciseEntry : basePreciseMap.entrySet()) {
                    basePreciseBsonFilters.add(eq(TAG_PREFIX + basePreciseEntry.getKey(), basePreciseEntry.getValue()));
                }
                return and(basePreciseBsonFilters);
            case Precise:
                PreciseTagFilter preciseTagFilter = (PreciseTagFilter) tagFilter;
                List<Bson> preciseBsonFilters = new ArrayList<>();
                List<BasePreciseTagFilter> basePreciseTagFilters = preciseTagFilter.getChildren();
                for (BasePreciseTagFilter subBasePreciseTagFilter : basePreciseTagFilters) {
                    preciseBsonFilters.add(genTagKVBson(subBasePreciseTagFilter));
                }
                return or(preciseBsonFilters);
            case WithoutTag:
            default:
                return null;
        }
    }

    private TaskExecuteResult executeProjectTask(TimeInterval timeInterval, String storageUnit, Project project) {
        MongoCollection<Document> collection = getCollection(storageUnit);
        if (collection == null) {
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("create collection failure!"));
        }
        try (MongoCursor<Document> cursor = collection.find(
            and(
                genPatternBson(project.getPatterns()),
                genTagKVBson(project.getTagFilter())
            )
        ).projection(
            fields(
                excludeId(),
                include(NAME, TYPE, VALUES)
            )
        ).iterator()) {
            MongoDBQueryRowStream rowStream = new MongoDBQueryRowStream(cursor, timeInterval);
            return new TaskExecuteResult(rowStream);
        }
    }

    private TaskExecuteResult executeDeleteTask(String storageUnit, Delete delete) {
        MongoCollection<Document> collection = getCollection(storageUnit);
        if (collection == null) {
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("create collection failure!"));
        }
        // TODO: @zhanglingzhe
        return null;
    }

    private Exception insertRowRecords(RowDataView data, String storageUnit) {
        MongoCollection<Document> collection = getCollection(storageUnit);
        if (collection == null) {
            return new PhysicalTaskExecuteFailureException("create collection failure!");
        }
        // TODO: @zhanglingzhe
        return null;
    }

    private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
        MongoCollection<Document> collection = getCollection(storageUnit);
        if (collection == null) {
            return new PhysicalTaskExecuteFailureException("create collection failure!");
        }
        // TODO: @zhanglingzhe
        return null;
    }

    @Override
    public List<Timeseries> getTimeSeries() {
        // TODO: @zhanglingzhe
        return null;
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) {
        // DOESN'T NEED TO IMPLEMENT
        return null;
    }

    @Override
    public void release() {
        collectionMap = null;
        mongoDatabase = null;
        mongoClient.close();
        mongoClient = null;
    }
}
