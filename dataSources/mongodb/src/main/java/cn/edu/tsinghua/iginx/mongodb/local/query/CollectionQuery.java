package cn.edu.tsinghua.iginx.mongodb.local.query;

import cn.edu.tsinghua.iginx.mongodb.local.entity.PathTree;
import cn.edu.tsinghua.iginx.mongodb.local.entity.ResultTable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.util.*;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

public class CollectionQuery {

  private final MongoCollection<BsonDocument> collection;

  private static final ThreadLocal<Map<String, Integer>> parseIntCache = new ThreadLocal<>();
  private static final ThreadLocal<Map<Integer, String>> encodeIntCache = new ThreadLocal<>();

  public CollectionQuery(MongoCollection<BsonDocument> collection) {
    this.collection = collection;
    parseIntCache.set(new HashMap<>());
    encodeIntCache.set(new HashMap<>());
  }

  public ResultTable query(PathTree tree) {
    tree.setLeaf(false);
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
      BsonValue subProjection = getSubProjection(child.getValue());
      projection.put(child.getKey(), subProjection);
    }

    return projection;
  }

  private ResultTable getResult(MongoCursor<BsonDocument> cursor, PathTree tree) {
    String datebaseName = this.collection.getNamespace().getDatabaseName();
    String collectionName = this.collection.getNamespace().getCollectionName();
    List<String> prefixes = new ArrayList<>(Arrays.asList(datebaseName, collectionName));

    ResultTable.Builder builder = new ResultTable.Builder();
    putFieldNames(builder, prefixes, tree);

    while (cursor.hasNext()) {
      BsonDocument doc = cursor.next();
      long key = doc.remove("$recordId").asInt64().getValue();
      builder.switchKey(key);
      putMatchedFields(builder, prefixes, tree, doc);
    }

    return builder.build();
  }

  private void putFieldNames(ResultTable.Builder builder, List<String> prefixes, PathTree tree) {
    if (tree.isLeaf()) {
      builder.put(String.join(".", prefixes));
    }

    for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
      if (child.getKey() != null) {
        prefixes.add(child.getKey());
        putFieldNames(builder, prefixes, child.getValue());
        prefixes.remove(prefixes.size() - 1);
      }
    }
  }

  private void putMatchedFields(
      ResultTable.Builder builder, List<String> prefixes, PathTree tree, BsonValue value) {
    if (tree.isLeaf()) {
      builder.put(String.join(".", prefixes), value);
    }

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
      ResultTable.Builder builder, List<String> prefixes, PathTree tree, BsonDocument doc) {
    for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
      String node = child.getKey();
      PathTree subTree = child.getValue();
      if (node == null) {
        putWildcardMatchedSubFields(builder, prefixes, subTree, doc);
      } else {
        BsonValue subRaw = doc.get(node);
        if (subRaw != null) {
          prefixes.add(node);
          putMatchedFields(builder, prefixes, subTree, subRaw);
          prefixes.remove(prefixes.size() - 1);
        }
      }
    }
  }

  private void putWildcardMatchedSubFields(
      ResultTable.Builder builder, List<String> prefixes, PathTree subTree, BsonDocument doc) {
    PathTree wildcardSubTree = new PathTree();
    wildcardSubTree.getChildren().put(null, subTree);

    for (Map.Entry<String, BsonValue> field : doc.entrySet()) {
      prefixes.add(field.getKey());
      putMatchedFields(builder, prefixes, subTree, field.getValue());
      putMatchedFields(builder, prefixes, wildcardSubTree, field.getValue());
      prefixes.remove(prefixes.size() - 1);
    }
  }

  private void putMatchedSubFields(
      ResultTable.Builder builder, List<String> prefixes, PathTree tree, BsonArray arr) {
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
      ResultTable.Builder builder, List<String> prefixes, PathTree subTree, BsonArray arr) {
    PathTree wildcardSubTree = new PathTree();
    wildcardSubTree.getChildren().put(null, subTree);

    for (int idx = 0; idx < arr.size(); idx++) {
      String node = encodeInt(idx);
      BsonValue subRaw = arr.get(idx);
      prefixes.add(node);
      putMatchedFields(builder, prefixes, subTree, subRaw);
      putMatchedFields(builder, prefixes, wildcardSubTree, subRaw);
      prefixes.remove(prefixes.size() - 1);
    }
  }

  private Integer parseInt(String s) {
    return parseIntCache
        .get()
        .computeIfAbsent(
            s,
            str -> {
              try {
                return Integer.parseInt(str);
              } catch (Exception e) {
                return null;
              }
            });
  }

  private String encodeInt(int i) {
    return encodeIntCache.get().computeIfAbsent(i, String::valueOf);
  }

  private void putMatchedSubSubFieldsAsArray(
      ResultTable.Builder builder, List<String> prefixes, PathTree subTree, BsonArray raw) {
    Builder subBuilder = new Builder();

    for (BsonValue subRaw : raw) {
      putMatchedFields(subBuilder, prefixes, subTree, subRaw);
    }

    for (Map.Entry<String, BsonArray> value : subBuilder.getArrays().entrySet()) {
      builder.put(value.getKey(), value.getValue());
    }
  }

  private static class Builder extends ResultTable.Builder {

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
