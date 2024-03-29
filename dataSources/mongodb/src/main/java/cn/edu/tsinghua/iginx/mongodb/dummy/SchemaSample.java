/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import java.util.*;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

public class SchemaSample {

  private final int schemaSampleSize;
  private final PathTree tree = new PathTree();

  public SchemaSample(int schemaSampleSize) {
    this.schemaSampleSize = schemaSampleSize;
    this.tree.put(Collections.singletonList("*").iterator());
  }

  public Map<String, DataType> query(MongoCollection<BsonDocument> collection) {
    AggregateIterable<BsonDocument> sampleResult =
        collection.aggregate(Collections.singletonList(getSampleStage()));

    int count = 0;
    ResultTable.Builder builder = new ResultTable.Builder();

    try (MongoCursor<BsonDocument> cursor = sampleResult.cursor()) {
      while (cursor.hasNext()) {
        BsonDocument doc = cursor.next();
        builder.add(count++, doc, tree);
      }
    }

    String[] prefixes =
        new String[] {
          collection.getNamespace().getDatabaseName(),
          collection.getNamespace().getCollectionName(),
        };

    ResultTable sampleTable = builder.build(prefixes);

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
    return Aggregates.sample(this.schemaSampleSize);
  }
}
