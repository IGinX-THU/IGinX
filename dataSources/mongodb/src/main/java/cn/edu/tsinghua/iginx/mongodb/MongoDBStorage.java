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
import cn.edu.tsinghua.iginx.mongodb.query.entity.MongoDBSchema;
import cn.edu.tsinghua.iginx.mongodb.tools.DataUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

// due to its schema, mongodb doesn't support history data
public class MongoDBStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBStorage.class.getName());

    private static final String STORAGE_ENGINE = "mongodb";

    private static final String CONNECTION_STRING = "mongodb://%s:%d";

    private static final String DATABASE = "IGinX";

    public static final String NAME = "name";

    public static final String FULLNAME = "fullname";

    public static final String TAG_PREFIX = "tag_";

    public static final String VALUES = "values";

    public static final String TYPE = "type";

    public static final String INNER_TIMESTAMP = "t";

    public static final String INNER_VALUE = "v";

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
                include(FULLNAME, NAME, TYPE, VALUES)
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
        checkNoTagFilter(delete);  // 删除暂时不支持精确到 tagkv
        if (delete.getTimeRanges() == null || delete.getTimeRanges().size() == 0) { // 没有传任何 time range
            List<String> paths = delete.getPatterns();
            if (paths.size() == 1 && paths.get(0).equals("*") && delete.getTagFilter() == null) {
                collection.drop();
            } else {
                // 整条序列级别的删除
                collection.deleteMany(genPatternBson(delete.getPatterns()));
            }
        } else {
            // 删除序列的一部分
            for (TimeRange range : delete.getTimeRanges()) {
                collection.deleteMany(and(
                    gte(NAME, range.getActualBeginTime()),
                    lte(NAME, range.getActualEndTime()),
                    genPatternBson(delete.getPatterns())
                ));
            }
        }
        return new TaskExecuteResult(null, null);
    }

    private void checkNoTagFilter(Delete delete) {
        if (delete.getTagFilter() != null) {
            throw new IllegalArgumentException("mongodb doesn't support delete with tag filter");
        }
    }

    private Exception insertRowRecords(RowDataView data, String storageUnit) {
        MongoCollection<Document> collection = getCollection(storageUnit);
        if (collection == null) {
            return new PhysicalTaskExecuteFailureException("create collection failure!");
        }
        List<MongoDBSchema> schemas = new ArrayList<>();
        for (int i = 0; i < data.getPathNum(); i++) {
            schemas.add(new MongoDBSchema(data.getPath(i), data.getTags(i), data.getDataType(i)));
        }

        Map<MongoDBSchema, List<JSONObject>> points = new HashMap<>();
        for (int i = 0; i < data.getTimeSize(); i++) {
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            for (int j = 0; j < data.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    MongoDBSchema schema = schemas.get(j);
                    List<JSONObject> timeAndValues = points.computeIfAbsent(schema, k -> new ArrayList<>());
                    Map<String, Object> timeAndValueMap = new HashMap<>();
                    timeAndValueMap.put(MongoDBStorage.INNER_TIMESTAMP, data.getKey(i));
                    timeAndValueMap.put(MongoDBStorage.INNER_VALUE, data.getValue(i, index));
                    timeAndValues.add(new JSONObject(timeAndValueMap));
                    index++;
                }
            }
        }

        try {
            for (Map.Entry<MongoDBSchema, List<JSONObject>> entry : points.entrySet()) {
                MongoDBSchema mongoDBSchema = entry.getKey();
                String fullName = mongoDBSchema.getName() + "{" + mongoDBSchema.getTags() + "}";
                List<JSONObject> jsonObjects = entry.getValue();
                Bson findQuery = eq(FULLNAME, fullName);
                if (collection.find(findQuery).iterator().hasNext()) {
                    collection.findOneAndUpdate(findQuery, Updates.pushEach(VALUES, jsonObjects));
                } else {
                    collection.insertOne(DataUtils.constructDocument(mongoDBSchema, mongoDBSchema.getType(), jsonObjects));
                }
            }
        } catch (Exception e) {
            logger.error("encounter error when write points to mongodb: ", e);
        }
        return null;
    }

    private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
        MongoCollection<Document> collection = getCollection(storageUnit);
        if (collection == null) {
            return new PhysicalTaskExecuteFailureException("create collection failure!");
        }
        List<Document> points = new ArrayList<>();
        for (int i = 0; i < data.getPathNum(); i++) {
            MongoDBSchema schema = new MongoDBSchema(data.getPath(i), data.getTags(i), data.getDataType(i));
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            List<JSONObject> jsonObjects = new ArrayList<>();
            for (int j = 0; j < data.getTimeSize(); j++) {
                if (bitmapView.get(j)) {
                    Map<String, Object> timeAndValueMap = new HashMap<>();
                    timeAndValueMap.put(MongoDBStorage.INNER_TIMESTAMP, data.getKey(j));
                    timeAndValueMap.put(MongoDBStorage.INNER_VALUE, data.getValue(i, index));
                    jsonObjects.add(new JSONObject(timeAndValueMap));
                    index++;
                }
            }
            String fullName = schema.getName() + "{" + schema.getTags() + "}";
            Bson findQuery = eq(FULLNAME, fullName);
            if (collection.find(findQuery).iterator().hasNext()) {
                collection.findOneAndUpdate(findQuery, Updates.pushEach(VALUES, jsonObjects));
            } else {
                collection.insertOne(DataUtils.constructDocument(schema, schema.getType(), jsonObjects));
            }
        }

        try {
            collection.insertMany(points);
        } catch (Exception e) {
            logger.error("encounter error when write points to influxdb: ", e);
        }
        return null;
    }

    @Override
    public List<Timeseries> getTimeSeries() {
        Set<String> storageUnits = new HashSet<>(collectionMap.keySet());
        Map<String, Map<String, DataType>> deDupMap = new HashMap<>();
        for (String storageUnit : storageUnits) {
            MongoCollection<Document> collection = getCollection(storageUnit);
            try (MongoCursor<Document> cursor = collection.find().projection(
                fields(
                    excludeId(),
                    include(TYPE, NAME, FULLNAME)
                )
            ).iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    String name = document.getString(NAME);
                    DataType dataType = DataUtils.fromString(document.getString(TYPE));
                    String fullName = document.getString(FULLNAME);
                    String tagString = "";
                    if (fullName.length() != name.length()) {
                        tagString = fullName.substring(name.length(), fullName.length() - 1);
                    }
                    Map<String, DataType> dupMap = deDupMap.computeIfAbsent(name, key -> new HashMap<>());
                    dupMap.put(tagString, dataType);
                }
            }
        }
        List<Timeseries> timeseriesList = new ArrayList<>();
        for (String name : deDupMap.keySet()) {
            Map<String, DataType> dupMap = deDupMap.get(name);
            for (String tagString : dupMap.keySet()) {
                DataType dataType = dupMap.get(tagString);
                if (tagString == null || tagString.isEmpty()) {
                    timeseriesList.add(new Timeseries(name, dataType));
                } else {
                    timeseriesList.add(new Timeseries(name, dataType, MongoDBSchema.resolveTagsFromString(tagString)));
                }
            }
        }
        return timeseriesList;
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
