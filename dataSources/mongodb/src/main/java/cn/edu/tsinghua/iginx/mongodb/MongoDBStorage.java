package cn.edu.tsinghua.iginx.mongodb;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.DataArea;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.mongodb.entity.MongoId;
import cn.edu.tsinghua.iginx.mongodb.entity.MongoRow;
import cn.edu.tsinghua.iginx.mongodb.entity.MongoTable;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

// due to its schema, mongodb doesn't support history data
public class MongoDBStorage implements IStorage {

    private static final String STORAGE_ENGINE = "mongodb";
    private static final long MAX_WAIT_TIME = 5;
    private static final int SESSION_POOL_MAX_SIZE = 200;

    private static final String DEFAULT_COLLECTION_NAME = "default";

    private final MongoClient client;

    public MongoDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }

        try {
            this.client = connect(meta.getIp(), meta.getPort());
        } catch (Exception e) {
            throw new StorageInitializationException("connection failure: " + e);
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
    public TaskExecuteResult executeProject(Project project, DataArea dataArea) {
        // TODO
        return new TaskExecuteResult(new PhysicalException("not implemented"));
    }

    @Override
    public TaskExecuteResult executeProjectDummy(Project project, DataArea dataArea) {
        // TODO
        return new TaskExecuteResult(new PhysicalException("not implemented"));
    }

    @Override
    public boolean isSupportProjectWithSelect() {
        // TODO
        return false;
    }

    @Override
    public TaskExecuteResult executeProjectWithSelect(
            Project project, Select select, DataArea dataArea) {
        // TODO
        return new TaskExecuteResult(new PhysicalException("not implemented"));
    }

    @Override
    public TaskExecuteResult executeProjectDummyWithSelect(
            Project project, Select select, DataArea dataArea) {
        // TODO
        return new TaskExecuteResult(new PhysicalException("not implemented"));
    }

    @Override
    public TaskExecuteResult executeDelete(Delete delete, DataArea dataArea) {
        // TODO
        return new TaskExecuteResult(new PhysicalException("not implemented"));
    }

    @Override
    public TaskExecuteResult executeInsert(Insert insert, DataArea dataArea) {
        try {
            List<WriteModel<Document>> operations = new ArrayList<>();
            for (MongoRow row : new MongoTable(insert.getData())) {
                // { "_id": { "$eq": { "_": <key>, "<tag0k>": <tag0v>,  "<tag1k>": <tag1v>, ...} }}
                Bson filter = new BsonDocument(MongoId.ID_FIELD_NAME, new BsonDocument("$eq", row.getBsonId()));
                // { "$set": { "<path0>": <value0>, "<path1>": <value1>, ... }}
                Bson update = new BsonDocument("$set", row.getBsonFields());
                operations.add(new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true)));
            }
            this.getCollection(dataArea).bulkWrite(operations);
        } catch (Exception e) {
            return new TaskExecuteResult(new PhysicalException("failed to insert", e));
        }

        return new TaskExecuteResult(null, null);
    }

    @Override
    public List<Column> getColumns() {
        // TODO
        return null;
    }

    @Override
    public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix) {
        // TODO
        return null;
    }

    private MongoCollection<Document> getCollection(DataArea dataArea) {
        String unitName = dataArea.getStorageUnit();
        String dbName = "iginx_" + unitName;
        return this.client.getDatabase(dbName).getCollection(DEFAULT_COLLECTION_NAME);
    }

    @Override
    public void release() {
        client.close();
    }
}
