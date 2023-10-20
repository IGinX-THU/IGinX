package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import com.mongodb.client.*;
import java.util.*;
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
    PathTree pathTree = new PathTree();
    if (patterns != null) {
      for (String pattern : patterns) {
        pathTree.put(Arrays.stream(pattern.split("\\.")).iterator());
      }
    }

    Map<String, PathTree> trees = getDatabaseTrees(pathTree);
    List<ResultTable> tables = new ArrayList<>();
    for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
      String dbName = tree.getKey();
      Filter predicateFilter = FilterUtils.EMTPY_FILTER;
      if (trees.size() == 1) {
        try {
          predicateFilter = FilterUtils.removeSamePrefix(filter, dbName);
        } catch (Exception ignored) {
        }
      }
      MongoDatabase db = this.client.getDatabase(dbName);
      List<ResultTable> dbResultList =
          new DatabaseQuery(db).query(tree.getValue(), predicateFilter);
      tables.addAll(dbResultList);
    }

    return new QueryRowStream(tables, filter);
  }

  private Map<String, PathTree> getDatabaseTrees(PathTree pathTree) {
    if (pathTree.hasWildcardChild()) {
      List<String> names = MongoDBStorage.getDatabaseNames(this.client);
      pathTree.eliminateWildcardChild(names);
    }

    return pathTree.getChildren();
  }

  private static class DatabaseQuery {
    private final MongoDatabase database;

    public DatabaseQuery(MongoDatabase database) {
      this.database = database;
    }

    public List<ResultTable> query(PathTree pathTree, Filter filter) {
      Map<String, PathTree> trees = getCollectionTrees(pathTree);

      List<ResultTable> resultList = new ArrayList<>();
      for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
        String collName = tree.getKey();
        Filter predicateFilter = FilterUtils.EMTPY_FILTER;
        if (trees.size() == 1) {
          try {
            predicateFilter = FilterUtils.removeSamePrefix(filter, collName);
          } catch (Exception ignored) {
          }
        }
        MongoCollection<BsonDocument> collection =
            this.database.getCollection(collName, BsonDocument.class);
        ResultTable dbResultList =
            new CollectionQuery(collection).query(tree.getValue(), predicateFilter);
        resultList.add(dbResultList);
      }

      return resultList;
    }

    private Map<String, PathTree> getCollectionTrees(PathTree pathTree) {
      if (pathTree.hasWildcardChild()) {
        List<String> names = this.database.listCollectionNames().into(new ArrayList<>());
        pathTree.eliminateWildcardChild(names);
      }

      return pathTree.getChildren();
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
        return builder.build(prefixes);
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
