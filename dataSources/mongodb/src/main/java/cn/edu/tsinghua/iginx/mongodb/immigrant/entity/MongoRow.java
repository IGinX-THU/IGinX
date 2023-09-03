package cn.edu.tsinghua.iginx.mongodb.immigrant.entity;

import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonValue;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MongoRow mongoRow = (MongoRow) o;
    return Objects.equals(id, mongoRow.id) && Objects.equals(fields, mongoRow.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, fields);
  }
}
