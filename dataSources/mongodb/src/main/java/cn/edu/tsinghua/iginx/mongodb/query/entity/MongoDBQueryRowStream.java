package cn.edu.tsinghua.iginx.mongodb.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.tools.DataUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.*;

public class MongoDBQueryRowStream implements RowStream {

    private final Table table;

    public MongoDBQueryRowStream(MongoCursor<Document> cursor, TimeInterval timeInterval) {
        Map<String, PriorityQueue<MongoDBPoint>> queueMap = new LinkedHashMap<>();
        Set<Field> fieldList = new LinkedHashSet<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            String name = document.getString(MongoDBStorage.NAME);
            String fullname = document.getString(MongoDBStorage.FULLNAME);
            DataType dataType = DataUtils.fromString(document.getString(MongoDBStorage.TYPE));
            Document timeAndValueDocument = document.get(MongoDBStorage.VALUES, Document.class);
            long timestamp = timeAndValueDocument.getLong(MongoDBStorage.INNER_TIMESTAMP);
            if (timeInterval.getStartTime() <= timestamp && timestamp < timeInterval.getEndTime()) {
                Object value = null;
                switch (dataType) {
                    case INTEGER:
                        value = timeAndValueDocument.getInteger(MongoDBStorage.INNER_VALUE);
                        break;
                    case LONG:
                        value = timeAndValueDocument.getLong(MongoDBStorage.INNER_VALUE);
                        break;
                    case BOOLEAN:
                        value = timeAndValueDocument.getBoolean(MongoDBStorage.INNER_VALUE);
                        break;
                    case DOUBLE:
                        value = timeAndValueDocument.getDouble(MongoDBStorage.INNER_VALUE);
                        break;
                    case FLOAT:
                        double doubleValue = timeAndValueDocument.getDouble(MongoDBStorage.INNER_VALUE);
                        value = (float) doubleValue;
                        break;
                    case BINARY:
                        Binary binary = (Binary) timeAndValueDocument.get(MongoDBStorage.INNER_VALUE);
                        value = binary.getData();
                        break;
                }
                MongoDBPoint point = new MongoDBPoint(new Value(dataType, value), timestamp);
                queueMap.computeIfAbsent(name, key -> new PriorityQueue<>()).add(point);
            }

            if (fullname.length() > name.length()) {
                String tagKVStr = fullname.substring(name.length(), fullname.length() - 1);
                String[] tagKVs = tagKVStr.split(",");
                Map<String, String> tagKVMap = new HashMap<>();
                for (String tagKV : tagKVs) {
                    tagKVMap.put(tagKV.split("=")[0], tagKV.split("=")[1]);
                }
                fieldList.add(new Field(name, dataType, tagKVMap));
            } else {
                fieldList.add(new Field(name, dataType));
            }
        }

        List<Pair<String, PriorityQueue<MongoDBPoint>>> queueList = new LinkedList<>();
        for (Map.Entry<String, PriorityQueue<MongoDBPoint>> entry : queueMap.entrySet()) {
            queueList.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        Header header = new Header(Field.KEY, new ArrayList<>(fieldList));
        List<Row> rows = new ArrayList<>();
        int queueHasData = queueMap.size();
        while (queueHasData != 0) {
            long minTimestamp = Long.MAX_VALUE;
            for (Pair<String, PriorityQueue<MongoDBPoint>> mongoDBSchemaPriorityQueuePair : queueList) {
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
