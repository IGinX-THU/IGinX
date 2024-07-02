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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeFieldRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.*;
import java.util.*;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

public class SampleQuery {

  private final MongoClient client;

  private final int sampleSize;

  public SampleQuery(MongoClient client, int sampleSize) {
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("Sample size must be positive");
    }
    this.client = client;
    this.sampleSize = sampleSize;
  }

  public RowStream query(List<String> patterns, Filter filter) throws PhysicalException {
    PathTree pathTree = PathTree.of(patterns);

    Map<String, PathTree> trees = QueryUtils.getDatabaseTrees(client, pathTree);
    Map<MongoDatabase, Pair<PathTree, Filter>> databaseQueryArgs =
        QueryUtils.getDatabaseQueryArgs(this.client, trees, filter);

    List<RowStream> results = new ArrayList<>();
    for (Map.Entry<MongoDatabase, Pair<PathTree, Filter>> args : databaseQueryArgs.entrySet()) {
      MongoDatabase db = args.getKey();
      PathTree subtree = args.getValue().getK();
      Filter predicateFilter = args.getValue().getV();
      List<RowStream> dbResultList =
          new DatabaseQuery(db, sampleSize).query(subtree, predicateFilter);
      results.addAll(dbResultList);
    }

    RowStream stream;
    if (results.size() == 1) {
      stream = results.get(0);
    } else {
      stream = new MergeFieldRowStreamWrapper(results);
    }
    if (filter != null) {
      stream = new FilterRowStreamWrapper(stream, filter);
    }
    return stream;
  }

  private static class DatabaseQuery {
    private final MongoDatabase database;
    private final int sampleSize;

    public DatabaseQuery(MongoDatabase database, int sampleSize) {
      if (sampleSize <= 0) {
        throw new IllegalArgumentException("Sample size must be positive");
      }
      this.database = database;
      this.sampleSize = sampleSize;
    }

    public List<RowStream> query(PathTree pathTree, Filter filter) {
      Map<String, PathTree> trees = QueryUtils.getCollectionTrees(database, pathTree);
      Map<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> collQueryArgs =
          QueryUtils.getCollectionsQueryArgs(database, trees, filter);

      List<RowStream> resultList = new ArrayList<>();
      for (Map.Entry<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> args :
          collQueryArgs.entrySet()) {
        MongoCollection<BsonDocument> coll = args.getKey();
        PathTree subtree = args.getValue().getK();
        Filter predicateFilter = args.getValue().getV();
        List<RowStream> result =
            new CollectionQuery(coll, sampleSize).query(subtree, predicateFilter);
        resultList.addAll(result);
      }
      return resultList;
    }
  }

  private static class CollectionQuery {

    private final MongoCollection<BsonDocument> collection;
    private final int sampleSize;

    public CollectionQuery(MongoCollection<BsonDocument> collection, int sampleSize) {
      if (sampleSize <= 0) {
        throw new IllegalArgumentException("Sample size must be positive");
      }
      this.collection = collection;
      this.sampleSize = sampleSize;
    }

    public List<RowStream> query(PathTree tree, Filter filter) {
      SchemaSample sample = new SchemaSample(sampleSize, tree);
      Map<String, DataType> schema = sample.query(collection, false);

      Map<String, DataType> projectedSchema = new HashMap<>();
      for (Map.Entry<String, DataType> entry : schema.entrySet()) {
        String path = entry.getKey();
        List<String> nodes = Arrays.asList(path.split("\\."));
        if (tree.match(nodes.listIterator())) {
          projectedSchema.put(path, entry.getValue());
        }
      }

      if (projectedSchema.isEmpty()) {
        return Collections.emptyList();
      }

      Bson predicate = QueryUtils.getPredicate(filter);
      Bson projection = QueryUtils.getProjection(tree);
      FindIterable<BsonDocument> find = this.collection.find(predicate).projection(projection);
      MongoCursor<BsonDocument> cursor = find.cursor();

      String[] prefixes =
          new String[] {
            this.collection.getNamespace().getDatabaseName(),
            this.collection.getNamespace().getCollectionName(),
          };

      return Collections.singletonList(new FindRowStream(cursor, tree, projectedSchema, prefixes));
    }
  }
}
