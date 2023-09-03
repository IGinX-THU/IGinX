package cn.edu.tsinghua.iginx.mongodb.local.query;

import cn.edu.tsinghua.iginx.mongodb.local.entity.PathTree;
import cn.edu.tsinghua.iginx.mongodb.local.entity.ResultTable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;

public class DatabaseQuery {
  private final MongoDatabase database;

  public DatabaseQuery(MongoDatabase database) {
    this.database = database;
  }

  public List<ResultTable> query(PathTree pathTree) {
    Map<String, PathTree> trees = getCollectionTrees(pathTree);

    List<ResultTable> resultList = new ArrayList<>();
    for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
      MongoCollection<BsonDocument> collection =
          this.database.getCollection(tree.getKey(), BsonDocument.class);
      ResultTable dbResultList = new CollectionQuery(collection).query(tree.getValue());
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
