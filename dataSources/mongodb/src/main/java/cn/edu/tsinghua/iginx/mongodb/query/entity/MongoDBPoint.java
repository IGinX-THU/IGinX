package cn.edu.tsinghua.iginx.mongodb.query.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;

public class MongoDBPoint implements Comparable<MongoDBPoint> {

    public final Value value;

    public final long timestamp;

    public MongoDBPoint(Value value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public Value getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(MongoDBPoint o) {
        if (timestamp < o.timestamp) {
            return -1;
        } else if (timestamp > o.timestamp) {
            return 1;
        }
        return 0;
    }
}