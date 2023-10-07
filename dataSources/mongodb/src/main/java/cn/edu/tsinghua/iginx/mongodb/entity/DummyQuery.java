package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.client.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.*;
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
    List<Result> resultList = new ArrayList<>();
    for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
      MongoDatabase db = this.client.getDatabase(tree.getKey());
      List<Result> dbResultList = new DatabaseQuery(db).query(tree.getValue());
      resultList.addAll(dbResultList);
    }

    return joinByKeyOn(resultList, filter);
  }

  private Map<String, PathTree> getDatabaseTrees(PathTree pathTree) {
    if (pathTree.hasWildcardChild()) {
      List<String> names = this.client.listDatabaseNames().into(new ArrayList<>());
      pathTree.eliminateWildcardChild(names);
    }

    return pathTree.getChildren();
  }

  private RowStream joinByKeyOn(List<Result> tables, Filter condition) {
    return new QueryRowStream(tables, condition);
  }

  private static class QueryRowStream implements RowStream {

    private final Header header;

    private final Iterator<Long> keyItr;

    private final Map<Integer, Result> tables = new TreeMap<>();

    private final Filter condition;

    public QueryRowStream(List<Result> results, Filter condition) {
      List<Field> fields = new ArrayList<>();
      SortedSet<Long> keys = new TreeSet<>();
      for (Result result : results) {
        keys.addAll(result.keySet());
        this.tables.put(fields.size(), result);
        fields.addAll(result.getFields());
      }

      this.header = new Header(Field.KEY, fields);
      this.keyItr = keys.iterator();
      this.condition = condition;
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
      if (keyItr.hasNext()) {
        Long key = keyItr.next();
        Object[] values = new Object[header.getFieldSize()];
        for (Map.Entry<Integer, Result> offsetTable : tables.entrySet()) {
          int offset = offsetTable.getKey();
          Map<Integer, Object> sparseValues = offsetTable.getValue().getRow(key);
          if (sparseValues != null) {
            for (Map.Entry<Integer, Object> sparseValue : sparseValues.entrySet()) {
              int index = sparseValue.getKey();
              values[offset + index] = sparseValue.getValue();
            }
          }
        }
        return new Row(header, key, values);
      }
      return null;
    }
  }

  private static class DatabaseQuery {
    private final MongoDatabase database;

    public DatabaseQuery(MongoDatabase database) {
      this.database = database;
    }

    public List<Result> query(PathTree pathTree) {
      Map<String, PathTree> trees = getCollectionTrees(pathTree);

      List<Result> resultList = new ArrayList<>();
      for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
        MongoCollection<BsonDocument> collection =
            this.database.getCollection(tree.getKey(), BsonDocument.class);
        Result dbResultList = new CollectionQuery(collection).query(tree.getValue());
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

    private static final ThreadLocal<Map<String, Integer>> parseIntCache = new ThreadLocal<>();
    private static final ThreadLocal<Map<Integer, String>> encodeIntCache = new ThreadLocal<>();

    public CollectionQuery(MongoCollection<BsonDocument> collection) {
      this.collection = collection;
      parseIntCache.set(new HashMap<>());
      encodeIntCache.set(new HashMap<>());
    }

    public Result query(PathTree tree) {
      Bson projection = getProjection(tree);
      FindIterable<BsonDocument> find =
          this.collection.find().projection(projection).showRecordId(true);
      try (MongoCursor<BsonDocument> cursor = find.cursor()) {
        return getResult(cursor, tree);
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
        if (parseInt(node) != null) {
          return new BsonInt32(1);
        }
        BsonValue subProjection = getSubProjection(child.getValue());
        projection.put(node, subProjection);
      }

      return projection;
    }

    private Result getResult(MongoCursor<BsonDocument> cursor, PathTree tree) {
      String datebaseName = this.collection.getNamespace().getDatabaseName();
      String collectionName = this.collection.getNamespace().getCollectionName();
      List<String> prefixes = new ArrayList<>(Arrays.asList(datebaseName, collectionName));

      Result.Builder builder = new Result.Builder();

      while (cursor.hasNext()) {
        BsonDocument doc = cursor.next();
        long key = doc.remove("$recordId").asInt64().getValue();
        builder.switchKey(key);
        putMatchedFields(builder, prefixes, tree, doc);
      }

      return builder.build();
    }

    private void putMatchedFields(
        Result.Builder builder, List<String> prefixes, PathTree tree, BsonValue value) {
      switch (value.getBsonType()) {
        case DOCUMENT:
          putMatchedSubFields(builder, prefixes, tree, (BsonDocument) value);
          break;
        case ARRAY:
          putMatchedSubFields(builder, prefixes, tree, (BsonArray) value);
          break;
      }
    }

    private void putMatchedSubFields(
        Result.Builder builder, List<String> prefixes, PathTree tree, BsonDocument doc) {
      for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
        String node = child.getKey();
        PathTree subTree = child.getValue();
        if (node == null) {
          putWildcardMatchedSubFields(builder, prefixes, subTree, doc);
        } else {
          BsonValue subRaw = doc.get(node);
          if (subRaw != null) {
            prefixes.add(node);
            if (subTree.isLeaf()) {
              builder.put(String.join(".", prefixes), subRaw);
            }
            putMatchedFields(builder, prefixes, subTree, subRaw);
            prefixes.remove(prefixes.size() - 1);
          }
        }
      }
    }

    private void putWildcardMatchedSubFields(
        Result.Builder builder, List<String> prefixes, PathTree subTree, BsonDocument doc) {
      PathTree wildcardSubTree = new PathTree();
      wildcardSubTree.getChildren().put(null, subTree);

      for (Map.Entry<String, BsonValue> field : doc.entrySet()) {
        BsonValue value = field.getValue();
        prefixes.add(field.getKey());
        if (value.getBsonType().isContainer()) {
          putMatchedFields(builder, prefixes, subTree, value);
          putMatchedFields(builder, prefixes, wildcardSubTree, value);
        } else if (subTree.isLeaf()) {
          builder.put(String.join(".", prefixes), value);
        }
        prefixes.remove(prefixes.size() - 1);
      }
    }

    private void putMatchedSubFields(
        Result.Builder builder, List<String> prefixes, PathTree tree, BsonArray arr) {
      PathTree notIndexPart = new PathTree();

      for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
        String node = child.getKey();
        PathTree subTree = child.getValue();
        if (node == null) {
          putWildcardMatchedSubFields(builder, prefixes, subTree, arr);
        } else {
          Integer index = parseInt(node);
          if (index == null) {
            notIndexPart.getChildren().put(node, subTree);
          } else {
            if (0 <= index && index < arr.size()) {
              BsonValue subRaw = arr.get(index);
              prefixes.add(node);
              if (subTree.isLeaf()) {
                builder.put(String.join(".", prefixes), subRaw);
              }
              putMatchedFields(builder, prefixes, subTree, subRaw);
              prefixes.remove(prefixes.size() - 1);
            }
          }
        }
      }

      if (!notIndexPart.getChildren().isEmpty()) {
        putMatchedSubSubFieldsAsArray(builder, prefixes, notIndexPart, arr);
      }
    }

    private void putWildcardMatchedSubFields(
        Result.Builder builder, List<String> prefixes, PathTree subTree, BsonArray arr) {
      PathTree wildcardSubTree = new PathTree();
      wildcardSubTree.getChildren().put(null, subTree);

      for (int idx = 0; idx < arr.size(); idx++) {
        String node = encodeInt(idx);
        BsonValue value = arr.get(idx);
        prefixes.add(node);
        if (value.getBsonType().isContainer()) {
          putMatchedFields(builder, prefixes, subTree, value);
          putMatchedFields(builder, prefixes, wildcardSubTree, value);
        } else if (subTree.isLeaf()) {
          builder.put(String.join(".", prefixes), value);
        }
        prefixes.remove(prefixes.size() - 1);
      }
    }

    private Integer parseInt(String s) {
      Function<String, Integer> converter =
          str -> {
            try {
              return Integer.parseInt(str);
            } catch (Exception e) {
              return null;
            }
          };
      return parseIntCache.get().computeIfAbsent(s, converter);
    }

    private String encodeInt(int i) {
      return encodeIntCache.get().computeIfAbsent(i, String::valueOf);
    }

    private void putMatchedSubSubFieldsAsArray(
        Result.Builder builder, List<String> prefixes, PathTree tree, BsonArray raw) {
      Builder subBuilder = new Builder();

      for (BsonValue subRaw : raw) {
        putMatchedFields(subBuilder, prefixes, tree, subRaw);
      }

      for (Map.Entry<String, BsonArray> value : subBuilder.getArrays().entrySet()) {
        builder.put(value.getKey(), value.getValue());
      }
    }

    private static class Builder extends Result.Builder {

      private final Map<String, BsonArray> arrays = new HashMap<>();

      public Map<String, BsonArray> getArrays() {
        return arrays;
      }

      @Override
      public void put(String field, BsonValue value) {
        BsonArray currArray = arrays.computeIfAbsent(field, s -> new BsonArray());
        currArray.add(value);
      }
    }
  }

  private static class PathTree {

    private final Map<String, PathTree> children;

    private boolean leaf;

    public PathTree() {
      this.children = new HashMap<>();
      this.leaf = false;
    }

    public void put(Iterator<String> nodes) {
      if (!nodes.hasNext()) {
        leaf = true;
        return;
      }

      String node = nodes.next();
      if (NameUtils.isWildcard(node)) node = null;

      children.computeIfAbsent(node, k -> new PathTree()).put(nodes);
    }

    private void put(PathTree anotherTree) {
      if (anotherTree.leaf) {
        leaf = true;
      }

      for (Map.Entry<String, PathTree> child : anotherTree.children.entrySet()) {
        children.computeIfAbsent(child.getKey(), k -> new PathTree()).put(child.getValue());
      }
    }

    public Map<String, PathTree> getChildren() {
      return children;
    }

    public boolean isLeaf() {
      return leaf;
    }

    @Override
    public String toString() {
      return "PathTree{" + "children=" + children + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PathTree that = (PathTree) o;
      return Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
      return Objects.hash(children);
    }

    public boolean hasWildcardChild() {
      return children.containsKey(null);
    }

    public void eliminateWildcardChild(Iterable<String> allNodes) {
      PathTree subTree = children.remove(null);
      if (subTree == null) return;

      PathTree wildcardSubTree = new PathTree();
      wildcardSubTree.children.put(null, subTree);

      for (String node : allNodes) {
        PathTree child = children.computeIfAbsent(node, k -> new PathTree());
        child.put(subTree);
        child.put(wildcardSubTree);
      }
    }
  }

  private static class Result {
    private final List<Field> fields;

    private final Map<Long, SortedMap<Integer, Object>> data;

    private Result(List<Field> fields, Map<Long, SortedMap<Integer, Object>> data) {
      this.fields = fields;
      this.data = data;
    }

    public List<Field> getFields() {
      return fields;
    }

    public Set<Long> keySet() {
      return data.keySet();
    }

    public Map<Integer, Object> getRow(Long key) {
      return data.get(key);
    }

    public static class Builder {
      private final Map<String, Integer> fieldIndexes = new HashMap<>();

      private final Map<Integer, Object> lastValues = new HashMap<>();

      private final Map<Long, SortedMap<Integer, Object>> data = new HashMap<>();

      public Result build() {
        Field[] fields = new Field[fieldIndexes.size()];
        for (Map.Entry<String, Integer> fieldIndex : fieldIndexes.entrySet()) {
          Integer index = fieldIndex.getValue();
          Object lastValue = lastValues.get(index);
          DataType type = getType(lastValue);
          fields[index] = new Field(fieldIndex.getKey(), type);
        }

        return new Result(Arrays.asList(fields), data);
      }

      private DataType getType(Object lastValue) {
        if (lastValue != null) {
          if (lastValue instanceof Double) {
            return DataType.DOUBLE;
          } else if (lastValue instanceof Long) {
            return DataType.LONG;
          } else if (lastValue instanceof Integer) {
            return DataType.INTEGER;
          } else if (lastValue instanceof byte[]) {
            return DataType.BINARY;
          } else if (lastValue instanceof Boolean) {
            return DataType.BOOLEAN;
          }
        }
        return DataType.BINARY;
      }

      private long currentKey = 0;
      private Map<Integer, Object> currentRow = null;

      public void switchKey(long key) {
        this.currentKey = key;
        this.currentRow = null;
      }

      public void put(String field, BsonValue rawValue) {
        int index = fieldIndexes.computeIfAbsent(field, k -> fieldIndexes.size());
        Object value = this.getValue(rawValue);
        this.lastValues.put(index, value);
        this.getCurrentRow().put(index, value);
      }

      private Map<Integer, Object> getCurrentRow() {
        if (this.currentRow == null) {
          this.currentRow = this.data.computeIfAbsent(this.currentKey, k -> new TreeMap<>());
        }
        return this.currentRow;
      }

      private Object getValue(BsonValue value) {
        if (value.isString()) {
          return value.asString().getValue().getBytes();
        } else {
          Object o = convert(value);
          if (o instanceof String) {
            o = ((String) o).getBytes();
          }
          return o;
        }
      }

      private Object convert(BsonValue value) {
        switch (value.getBsonType()) {
          case DOUBLE:
            return ((BsonDouble) value).getValue();
          case STRING:
            return ((BsonString) value).getValue();
          case DOCUMENT:
            return ((BsonDocument) value).toJson();
          case ARRAY:
            return ((BsonArray) value)
                .stream()
                    .map(this::convert)
                    .map(Object::toString)
                    .collect(Collectors.joining(",", "[", "]"));
          case OBJECT_ID:
            return ((BsonObjectId) value).getValue().toHexString();
          case BOOLEAN:
            return ((BsonBoolean) value).getValue();
          case DATE_TIME:
            return ((BsonDateTime) value).getValue();
          case REGULAR_EXPRESSION:
            return ((BsonRegularExpression) value).getPattern();
          case INT32:
            return ((BsonInt32) value).getValue();
          case TIMESTAMP:
            return ((BsonTimestamp) value).getValue();
          case INT64:
            return ((BsonInt64) value).getValue();
          case DECIMAL128:
            return ((BsonDecimal128) value).getValue().toString();
        }
        return value.toString();
      }
    }
  }
}
