package cn.edu.tsinghua.iginx.mongodb.immigrant.entity;

import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public class MongoId {
  public static final String KEY_SUBFIELD = "_";
  // Note: why set place holder?
  //     因为在测试中要求，即使是空列也需要返回该列。目前现状是，删除了一条序列的所有数据，
  // 并不等于删除了一条序列；删除一条序列是一个特殊操作。
  public static final long PLACE_HOLDER_KEY = Long.MAX_VALUE; // 分片区间左开右闭，此值不处于任何分片
  private final long key;
  private final Map<String, String> tags;

  public MongoId(long key, Map<String, String> tags) {
    this.key = key;
    this.tags = tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MongoId mongoId = (MongoId) o;
    return key == mongoId.key && Objects.equals(tags, mongoId.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, tags);
  }

  public long getKey() {
    return key;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public BsonValue getBsonKey() {
    return new BsonInt64(this.key);
  }

  public BsonDocument getBsonTags() {
    BsonDocument tagsDocument = new BsonDocument();
    for (Map.Entry<String, String> tag : this.tags.entrySet()) {
      String key = NameUtils.encodeTagK(tag.getKey());
      BsonValue value = new BsonString(tag.getValue());
      tagsDocument.append(key, value);
    }
    return tagsDocument;
  }

  public BsonDocument toBsonDocument() {
    return getBsonTags().append(KEY_SUBFIELD, getBsonKey());
  }
}
