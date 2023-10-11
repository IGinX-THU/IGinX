package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.client.*;
import java.util.*;
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
    PathTree pathTree = new PathTree();
    if (patterns != null) {
      for (String pattern : patterns) {
        pathTree.put(Arrays.stream(pattern.split("\\.")).iterator());
      }
    }

    Map<String, PathTree> trees = getDatabaseTrees(pathTree);
    List<ResultTable> tables = new ArrayList<>();
    for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
      MongoDatabase db = this.client.getDatabase(tree.getKey());
      List<ResultTable> dbResultList = new DatabaseQuery(db).query(tree.getValue());
      tables.addAll(dbResultList);
    }

    return new QueryRowStream(tables, filter);
  }

  private Map<String, PathTree> getDatabaseTrees(PathTree pathTree) {
    if (pathTree.hasWildcardChild()) {
      List<String> names = this.client.listDatabaseNames().into(new ArrayList<>());
      pathTree.eliminateWildcardChild(names);
    }

    return pathTree.getChildren();
  }

  private static class DatabaseQuery {
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

  private static class CollectionQuery {

    private final MongoCollection<BsonDocument> collection;

    public CollectionQuery(MongoCollection<BsonDocument> collection) {
      this.collection = collection;
    }

    public ResultTable query(PathTree tree) {
      Bson projection = getProjection(tree);
      FindIterable<BsonDocument> find =
          this.collection.find().projection(projection).showRecordId(true);
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

  static class QueryRowStream implements RowStream {

    private final Header header;

    private final List<Map<Long, Object>> data;

    private final Filter condition;

    private final Iterator<Long> keyItr;

    public QueryRowStream(List<ResultTable> results, Filter condition) {
      List<Field> fields = new ArrayList<>();
      List<Map<Long, Object>> data = new ArrayList<>();
      SortedSet<Long> keys = new TreeSet<>();

      results.stream()
          .map(ResultTable::getColumns)
          .map(Map::entrySet)
          .flatMap(Collection::stream)
          .forEach(
              entry -> {
                String path = entry.getKey();
                ResultColumn column = entry.getValue();
                DataType type = column.getType();
                Map<Long, Object> columnData = column.getData();

                fields.add(new Field(path, type));
                data.add(columnData);
                keys.addAll(columnData.keySet());
              });

      this.header = new Header(Field.KEY, fields);
      this.data = data;
      this.condition = condition;
      this.keyItr = keys.iterator();
    }

    @Override
    public Header getHeader() {
      return header;
    }

    @Override
    public void close() {}

    private Row nextRow = null;

    @Override
    public boolean hasNext() throws PhysicalException {
      if (nextRow == null) {
        nextRow = getNextMatchRow();
      }

      return nextRow != null;
    }

    @Override
    public Row next() throws PhysicalException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Row curr = nextRow;
      nextRow = null;
      return curr;
    }

    private Row getNextMatchRow() throws PhysicalException {
      for (Row row = getNextRow(); row != null; row = getNextRow()) {
        if (FilterUtils.validate(this.condition, row)) {
          return row;
        }
      }
      return null;
    }

    private Row getNextRow() {
      if (!keyItr.hasNext()) {
        return null;
      }

      Long key = keyItr.next();
      Object[] values = new Object[header.getFieldSize()];
      for (int idx = 0; idx < data.size(); idx++) {
        values[idx] = data.get(idx).get(key);
      }
      return new Row(header, key, values);
    }
  }
}
