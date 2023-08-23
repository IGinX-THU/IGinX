package cn.edu.tsinghua.iginx.mongodb.query;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.mongodb.entity.MongoId;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.TypeUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import java.util.*;
import org.bson.*;

public class AllColumns implements Iterable<Column> {
  private final MongoCollection<BsonDocument> mongoCollection;

  private AllColumns(MongoCollection<BsonDocument> mongoCollection) {
    this.mongoCollection = mongoCollection;
  }

  public static AllColumns of(MongoCollection<BsonDocument> mongoCollection) {
    return new AllColumns(mongoCollection);
  }

  @Override
  public Iterator<Column> iterator() {
    // db.default.aggregate([
    //    { "$unset": "_id._" },
    //    { "$group": { "_id": "$_id", "last": { "$mergeObjects": {"$unsetField":{"field":
    // "_id","input": "$$ROOT",}} } } },
    // ] )
    AggregateIterable<BsonDocument> aggregate =
        mongoCollection.aggregate(
            Arrays.asList(
                new BsonDocument("$unset", new BsonString(MongoId.KEY_FIELD_NAME)),
                new BsonDocument(
                    "$group",
                    new BsonDocument(
                        Arrays.asList(
                            new BsonElement(
                                MongoId.ID_FIELD_NAME, new BsonString("$" + MongoId.ID_FIELD_NAME)),
                            new BsonElement(
                                "last",
                                new BsonDocument(
                                    "$mergeObjects",
                                    new BsonDocument(
                                        "$unsetField",
                                        new BsonDocument(
                                            Arrays.asList(
                                                new BsonElement(
                                                    "field", new BsonString(MongoId.ID_FIELD_NAME)),
                                                new BsonElement(
                                                    "input", new BsonString("$$ROOT"))))))))))));

    // result likes:
    // [
    //  {
    //    _id: {...},
    //    last: {
    //      fhdh_egbd_dhbd: Long("14999"),
    //      fhdh_egbd_dhcd: Long("15000"),
    //      fhdh_egbd_dhdd: Binary(Buffer.from("69326f51706f42745a42", "hex"), 0),
    //      ...
    //    }
    //  }
    // ]

    List<Column> columnList = new ArrayList<>();
    for (BsonDocument result : aggregate) {
      BsonDocument id = result.getDocument(MongoId.ID_FIELD_NAME);
      Map<String, String> tags = new TreeMap<>();
      for (Map.Entry<String, BsonValue> tag : id.entrySet()) {
        String tagk = NameUtils.decodeTagK(tag.getKey());
        String tagv = tag.getValue().asString().getValue();
        tags.put(tagk, tagv);
      }

      BsonDocument last = result.getDocument("last");
      for (Map.Entry<String, BsonValue> field : last.entrySet()) {
        String name = NameUtils.decodePath(field.getKey());
        DataType type = TypeUtils.getType(field.getValue().getBsonType());
        columnList.add(new Column(name, type, tags));
      }
    }

    return columnList.iterator();
  }
}
