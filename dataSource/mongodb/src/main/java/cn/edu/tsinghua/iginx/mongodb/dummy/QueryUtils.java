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

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtils.class);

  static Map<MongoDatabase, Pair<PathTree, Filter>> getDatabaseQueryArgs(
      MongoClient client, Map<String, PathTree> dbTrees, Filter filter) {
    Map<MongoDatabase, Pair<PathTree, Filter>> dbQueryArgs = new HashMap<>();
    for (Map.Entry<String, PathTree> tree : dbTrees.entrySet()) {
      String dbName = tree.getKey();
      Filter predicateFilter = FilterUtils.EMTPY_FILTER;
      if (dbTrees.size() == 1) {
        try {
          predicateFilter = FilterUtils.removeSamePrefix(filter, dbName);
        } catch (Exception ignored) {
        }
      }
      dbQueryArgs.put(client.getDatabase(dbName), new Pair<>(tree.getValue(), predicateFilter));
    }
    return dbQueryArgs;
  }

  static Map<String, PathTree> getDatabaseTrees(MongoClient client, PathTree pathTree) {
    List<String> names = MongoDBStorage.getDatabaseNames(client);
    pathTree.project(names);
    return pathTree.getChildren();
  }

  static Map<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> getCollectionsQueryArgs(
      MongoDatabase db, Map<String, PathTree> collTress, Filter filter) {
    Map<MongoCollection<BsonDocument>, Pair<PathTree, Filter>> collQueryArgs = new HashMap<>();
    for (Map.Entry<String, PathTree> tree : collTress.entrySet()) {
      String collName = tree.getKey();
      Filter predicateFilter = FilterUtils.EMTPY_FILTER;
      if (collTress.size() == 1) {
        try {
          predicateFilter = FilterUtils.removeSamePrefix(filter, collName);
        } catch (Exception ignored) {
        }
      }
      collQueryArgs.put(
          db.getCollection(collName, BsonDocument.class),
          new Pair<>(tree.getValue(), predicateFilter));
    }
    return collQueryArgs;
  }

  static Map<String, PathTree> getCollectionTrees(MongoDatabase database, PathTree pathTree) {
    List<String> names = database.listCollectionNames().into(new ArrayList<>());
    pathTree.project(names);
    return pathTree.getChildren();
  }

  public static Bson getPredicate(Filter filter) {
    Filter removedUnsupportedFilter =
        LogicalFilterUtils.superSetPushDown(
            filter,
            f -> {
              switch (f.getType()) {
                case Value:
                  return NameUtils.containNumberNode(((ValueFilter) f).getPath());
                case Path:
                  return NameUtils.containNumberNode(((PathFilter) f).getPathA())
                      || NameUtils.containNumberNode(((PathFilter) f).getPathB());
                case Key:
                case In:
                case Expr:
                  return true;
              }
              return false;
            });
    return FilterUtils.toBson(removedUnsupportedFilter);
  }

  static Bson getProjection(PathTree tree) {
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

  static BsonValue getSubProjection(PathTree tree) {
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
