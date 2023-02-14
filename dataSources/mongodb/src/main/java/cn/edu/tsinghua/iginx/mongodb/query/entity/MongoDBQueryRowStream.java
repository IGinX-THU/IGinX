package cn.edu.tsinghua.iginx.mongodb.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.tools.DataUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.MongoCursor;
import org.apache.arrow.flatbuf.Bool;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.*;

public class MongoDBQueryRowStream implements RowStream {

    private final Table table;

    public MongoDBQueryRowStream(MongoCursor<Document> cursor, Project project) {
        Map<String, Map<String, Pair<MongoDBSchema, Boolean>>> schemaCache = new HashMap<>();
        Map<MongoDBSchema, PriorityQueue<MongoDBPoint>> queueMap = new HashMap<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            String name = document.getString(MongoDBStorage.NAME);
            String tagString = document.getString(MongoDBStorage.TAGS);

            Map<String, Pair<MongoDBSchema, Boolean>> tagCache = schemaCache.computeIfAbsent(name, key -> new HashMap<>());
            Pair<MongoDBSchema, Boolean> pair = tagCache.computeIfAbsent(tagString, key -> {
                Map<String, String> tags = MongoDBSchema.resolveTagsFromString(key);
                boolean match = project.getTagFilter() == null || MongoDBSchema.match(tags, project.getTagFilter());
                MongoDBSchema schema = new MongoDBSchema(name, tags);
                return new Pair<>(schema, match);
            });
            if (!pair.v) { // 被 tag filter 过滤掉
                continue;
            }
            MongoDBSchema schema = pair.k;
            long timestamp = document.getLong(MongoDBStorage.TS);
            DataType dataType = DataUtils.fromString(document.getString(MongoDBStorage.TYPE));
            if (schema.getType() == null) {
                schema.setType(dataType);
            }
            Object value = null;
            switch (dataType) {
                case INTEGER:
                    value = document.getInteger(MongoDBStorage.VALUE);
                    break;
                case LONG:
                    value = document.getLong(MongoDBStorage.VALUE);
                case BOOLEAN:
                    value = document.getBoolean(MongoDBStorage.VALUE);
                    break;
                case DOUBLE:
                    value = document.getDouble(MongoDBStorage.VALUE);
                    break;
                case FLOAT:
                    double doubleValue = document.getDouble(MongoDBStorage.VALUE);
                    value = (float) doubleValue;
                    break;
                case BINARY:
                    Binary binary = (Binary) document.get(MongoDBStorage.VALUE);
                    value = binary.getData();
                    break;
            }
            MongoDBPoint point = new MongoDBPoint(new Value(dataType, value), timestamp);
            queueMap.computeIfAbsent(schema, key -> new PriorityQueue<>()).add(point);
        }
        List<Pair<MongoDBSchema, PriorityQueue<MongoDBPoint>>> queueList = new LinkedList<>();
        List<Field> fieldList = new ArrayList<>();
        for (MongoDBSchema schema: queueMap.keySet()) {
            queueList.add(new Pair<>(schema, queueMap.get(schema)));
            fieldList.add(schema.toField());
        }
        Header header = new Header(Field.KEY, fieldList);
        List<Row> rows = new ArrayList<>();
        int queueHasData = queueList.size();
        while (queueHasData != 0) {
            long minTimestamp = Long.MAX_VALUE;
            for (Pair<MongoDBSchema, PriorityQueue<MongoDBPoint>> mongoDBSchemaPriorityQueuePair : queueList) {
                PriorityQueue<MongoDBPoint> queue = mongoDBSchemaPriorityQueuePair.getV();
                if (queue.isEmpty()) {
                    continue;
                }
                minTimestamp = queue.peek().getTimestamp();
            }
            Object[] values = new Object[header.getFieldSize()];
            for (int i = 0; i < queueList.size(); i++) {
                PriorityQueue<MongoDBPoint> queue = queueList.get(i).getV();
                if (queue.isEmpty()) {
                    continue;
                }
                if (queue.peek().getTimestamp() == minTimestamp) {
                    MongoDBPoint point = queue.poll();
                    assert point != null;
                    values[i] = point.getValue().getValue();
                    if (queue.isEmpty()) {
                        queueHasData--;
                    }
                }
            }
            rows.add(new Row(header, minTimestamp, values));
        }
        this.table = new Table(header, rows);
    }

    @Override
    public Header getHeader() {
        return table.getHeader();
    }

    @Override
    public void close() {
        table.close();
    }

    @Override
    public boolean hasNext() {
        return table.hasNext();
    }

    @Override
    public Row next() {
        return table.next();
    }
}
