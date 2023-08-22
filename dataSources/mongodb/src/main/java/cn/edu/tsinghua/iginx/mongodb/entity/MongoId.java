package cn.edu.tsinghua.iginx.mongodb.entity;

import java.util.Map;
import java.util.Objects;

import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public class MongoId {
    public static final String ID_FIELD_NAME = "_id";
    public static final String KEY_SUBFIELD_NAME = "_";
    public static final String KEY_FIELD_NAME = ID_FIELD_NAME + "." + KEY_SUBFIELD_NAME;
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
        return getBsonTags().append(KEY_SUBFIELD_NAME, getBsonKey());
    }
}
