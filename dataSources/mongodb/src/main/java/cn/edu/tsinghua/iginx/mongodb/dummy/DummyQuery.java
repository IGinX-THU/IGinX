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

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

public class DummyQuery {

  private final MongoClient client;

  public DummyQuery(MongoClient client) {
    this.client = client;
  }

  public RowStream query(List<String> patterns, Filter filter) {
    PathTree pathTree = PathTree.of(patterns);

    Map<String, PathTree> trees = QueryUtils.getDatabaseTrees(client, pathTree);
    Map<MongoDatabase, Pair<PathTree, Filter>> getDatabaseQueryArgs =
        QueryUtils.getDatabaseQueryArgs(this.client, trees, filter);

    List<ResultTable> tables = new ArrayList<>();
    for (Map.Entry<MongoDatabase, Pair<PathTree, Filter>> args : getDatabaseQueryArgs.entrySet()) {
      MongoDatabase db = args.getKey();
      PathTree subtree = args.getValue().getK();
      Filter predicateFilter = args.getValue().getV();
      List<ResultTable> dbResultList = new DatabaseQuery(db).query(subtree, predicateFilter);
      tables.addAll(dbResultList);
    }

    return new QueryRowStream(tables, filter);
  }

  private static class DatabaseQuery {
    private final MongoDatabase database;

    public DatabaseQuery(MongoDatabase database) {
      this.database = database;
    }

    public List<ResultTable> query(PathTree pathTree, Filter filter) {
      Map<String, PathTree> trees = QueryUtils.getCollectionTrees(database, pathTree);
      Map<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> collQueryArgs =
          QueryUtils.getCollectionsQueryArgs(database, trees, filter);

      List<ResultTable> resultList = new ArrayList<>();
      for (Map.Entry<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> args :
          collQueryArgs.entrySet()) {
        MongoCollection<BsonDocument> collection = args.getKey();
        PathTree tree = args.getValue().getK();
        Filter predicateFilter = args.getValue().getV();
        ResultTable dbResultList = new CollectionQuery(collection).query(tree, predicateFilter);
        resultList.add(dbResultList);
      }
      return resultList;
    }
  }

  private static class CollectionQuery {

    private final MongoCollection<BsonDocument> collection;

    public CollectionQuery(MongoCollection<BsonDocument> collection) {
      this.collection = collection;
    }

    public ResultTable query(PathTree tree, Filter filter) {
      Bson predicate = getPredicate(filter);
      Bson projection = getProjection(tree);
      FindIterable<BsonDocument> find =
          this.collection.find(predicate).projection(projection).showRecordId(true);

      try (MongoCursor<BsonDocument> cursor = find.cursor()) {
        ResultTable.Builder builder = new ResultTable.Builder();
        while (cursor.hasNext()) {
          BsonDocument doc = cursor.next();
          long recordId = doc.remove("$recordId").asInt64().getValue();
          assert (0 <= recordId && recordId <= Integer.MAX_VALUE);
          builder.add(recordId << 32, doc, tree);
        }

        String[] prefixes =
            new String[] {
              this.collection.getNamespace().getDatabaseName(),
              this.collection.getNamespace().getCollectionName(),
            };
        return builder.build(prefixes, null);
      }
    }

    private static Bson getPredicate(Filter filter) {
      try {
        Filter removedKey = FilterUtils.tryIgnore(filter, f -> f.getType().equals(FilterType.Key));
        Filter removedNumberPath =
            FilterUtils.tryIgnore(
                removedKey,
                f -> {
                  switch (f.getType()) {
                    case Value:
                      return NameUtils.containNumberNode(((ValueFilter) f).getPath());
                    case Path:
                      return NameUtils.containNumberNode(((PathFilter) f).getPathA())
                          || NameUtils.containNumberNode(((PathFilter) f).getPathB());
                  }
                  return false;
                });
        return FilterUtils.toBson(removedNumberPath);
      } catch (Exception ignored) {
        return new Document();
      }
    }

    private Bson getProjection(PathTree tree) {
      if (tree.hasWildcardChild()) {
        return new BsonDocument();
      }

      BsonDocument projection = new BsonDocument();
      for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
        BsonValue subProjection = getSubProjection(child.getValue());
        projection.put(child.getKey(), subProjection);
      }

      return projection;
    }

    private BsonValue getSubProjection(PathTree tree) {
      if (tree.isLeaf() || tree.hasWildcardChild()) {
        return new BsonInt32(1);
      }

      BsonDocument projection = new BsonDocument();
      for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
        String node = child.getKey();
        if (TypeUtils.parseInt(node) != null) {
          return new BsonInt32(1);
        }
        BsonValue subProjection = getSubProjection(child.getValue());
        projection.put(node, subProjection);
      }

      return projection;
    }
  }
}
