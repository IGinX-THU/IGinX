package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

public class SchemaSample {

  private final int sampleSize;
  private final PathTree tree;

  public SchemaSample(int sampleSize) {
    this.sampleSize = sampleSize;
    PathTree tree = new PathTree();
    tree.put(Collections.singletonList("*").iterator());
    this.tree = tree;
  }

  public SchemaSample(int sampleSize, PathTree tree) {
    this.sampleSize = sampleSize;
    this.tree = tree;
  }

  public Map<String, DataType> query(MongoCollection<BsonDocument> collection, boolean hasPrefix) {
    AggregateIterable<BsonDocument> sampleResult =
        collection.aggregate(Collections.singletonList(getSampleStage()));

    long count = 0;
    ResultTable.Builder builder = new ResultTable.Builder();

    try (MongoCursor<BsonDocument> cursor = sampleResult.cursor()) {
      while (cursor.hasNext()) {
        BsonDocument doc = cursor.next();
        builder.add(count << 32, doc, tree);
        count++;
      }
    }

    String[] prefixes;
    if (hasPrefix) {
      prefixes =
          new String[] {
            collection.getNamespace().getDatabaseName(),
            collection.getNamespace().getCollectionName(),
          };
    } else {
      prefixes = new String[0];
    }

    ResultTable sampleTable = builder.build(prefixes, null);

    Map<String, ResultColumn> columns = sampleTable.getColumns();
    Map<String, DataType> schema = new HashMap<>(columns.size());

    for (Map.Entry<String, ResultColumn> entry : columns.entrySet()) {
      String path = entry.getKey();
      ResultColumn column = entry.getValue();
      DataType dataType = column.getType();
      schema.put(path, dataType);
    }

    return schema;
  }

  private Bson getSampleStage() {
    return Aggregates.sample(this.sampleSize);
  }
}
