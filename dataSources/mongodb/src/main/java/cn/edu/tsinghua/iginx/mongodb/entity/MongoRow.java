package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import java.util.Map;
import org.bson.*;

public class MongoRow {

  private final MongoId id;
  private final Map<String, MongoPoint> fields;

  public MongoRow(MongoId id, Map<String, MongoPoint> fields) {
    this.id = id;
    this.fields = fields;
  }

  public MongoId getId() {
    return id;
  }

  public Map<String, MongoPoint> getFields() {
    return fields;
  }

  public BsonDocument getBsonFields() {
    BsonDocument pointBsonDocument = new BsonDocument();
    for (Map.Entry<String, MongoPoint> point : this.fields.entrySet()) {
      String key = NameUtils.encodePath(point.getKey());
      BsonValue value = point.getValue().getBsonValue();
      pointBsonDocument.append(key, value);
    }
    return pointBsonDocument;
  }

  public BsonDocument getBsonId() {
    return id.toBsonDocument();
  }
}
