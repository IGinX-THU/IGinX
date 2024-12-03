/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
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
    Map<String, DataType> schemaHint = analyzeSchemaHintFromFilter(filter);

    List<ResultTable> tables = new ArrayList<>();
    for (Map.Entry<MongoDatabase, Pair<PathTree, Filter>> args : getDatabaseQueryArgs.entrySet()) {
      MongoDatabase db = args.getKey();
      PathTree subtree = args.getValue().getK();
      Filter predicateFilter = args.getValue().getV();

      List<ResultTable> dbResultList =
          new DatabaseQuery(db).query(subtree, predicateFilter, schemaHint);
      tables.addAll(dbResultList);
    }

    return new QueryRowStream(tables, filter);
  }

  private static Map<String, DataType> analyzeSchemaHintFromFilter(Filter filter) {
    Map<String, DataType> schema = new HashMap<>();
    filter.accept(
        new FilterVisitor() {
          @Override
          public void visit(AndFilter filter) {}

          @Override
          public void visit(OrFilter filter) {}

          @Override
          public void visit(NotFilter filter) {}

          @Override
          public void visit(KeyFilter filter) {}

          @Override
          public void visit(ValueFilter filter) {
            schema.put(filter.getPath(), filter.getValue().getDataType());
          }

          @Override
          public void visit(PathFilter filter) {}

          @Override
          public void visit(BoolFilter filter) {}

          @Override
          public void visit(ExprFilter filter) {}

          @Override
          public void visit(InFilter filter) {
            if (!filter.getValues().isEmpty()) {
              schema.put(filter.getPath(), filter.getValues().iterator().next().getDataType());
            }
          }
        });
    return schema;
  }

  private static class DatabaseQuery {
    private final MongoDatabase database;

    public DatabaseQuery(MongoDatabase database) {
      this.database = database;
    }

    public List<ResultTable> query(
        PathTree pathTree, Filter filter, Map<String, DataType> schemaHint) {
      Map<String, PathTree> trees = QueryUtils.getCollectionTrees(database, pathTree);
      Map<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> collQueryArgs =
          QueryUtils.getCollectionsQueryArgs(database, trees, filter);

      List<ResultTable> resultList = new ArrayList<>();
      for (Map.Entry<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> args :
          collQueryArgs.entrySet()) {
        MongoCollection<BsonDocument> collection = args.getKey();
        PathTree tree = args.getValue().getK();
        Filter predicateFilter = args.getValue().getV();
        ResultTable dbResultList =
            new CollectionQuery(collection).query(tree, predicateFilter, schemaHint);
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

    public ResultTable query(PathTree tree, Filter filter, Map<String, DataType> schemaHint) {
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

        String prefix = String.join(".", prefixes) + ".";
        Map<String, DataType> schemaHintWithoutPrefix = new HashMap<>();
        schemaHint.forEach(
            (k, v) -> {
              if (k.startsWith(prefix)) {
                schemaHintWithoutPrefix.put(k.substring(prefix.length()), v);
              }
            });

        return builder.build(prefixes, schemaHintWithoutPrefix);
      }
    }

    private static Bson getPredicate(Filter filter) {
      return QueryUtils.getPredicate(filter);
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
