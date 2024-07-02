/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
