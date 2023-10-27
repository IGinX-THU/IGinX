package cn.edu.tsinghua.iginx.mongodb.dummy;

import java.util.*;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

class ResultTable {

  private final Map<String, ResultColumn> columns;

  private ResultTable(Map<String, ResultColumn> columns) {
    this.columns = columns;
  }

  public Map<String, ResultColumn> getColumns() {
    return columns;
  }

  static class Builder {

    private final Map<List<String>, ResultColumn.Builder> builders = new HashMap<>();

    ResultTable build(String[] prefix) {
      Map<String, ResultColumn> columns = new TreeMap<>();
      for (Map.Entry<List<String>, ResultColumn.Builder> columnBuilder : builders.entrySet()) {
        String path = String.join(".", columnBuilder.getKey());
        if (prefix.length > 0) {
          path = String.join(".", prefix) + "." + path;
        }
        ResultColumn column = columnBuilder.getValue().build();
        columns.put(path, column);
      }
      return new ResultTable(columns);
    }

    public void add(long index, BsonDocument document, PathTree tree) {
      ResultRow row = new ResultRow();
      List<String> prefix = new ArrayList<>();
      put(row, document, tree, prefix);
      row.fillInto(builders, index, new ArrayList<>());
    }

    private int depth = 0;

    private void put(ResultRow dst, BsonValue value, PathTree tree, List<String> prefix) {
      if (tree.isLeaf() && !value.getBsonType().isContainer()) {
        List<String> pathPrefix = new ArrayList<>(prefix);
        dst.add(pathPrefix, value);
      }
      switch (value.getBsonType()) {
        case DOCUMENT:
          putDocument(dst, (BsonDocument) value, tree, prefix);
          break;
        case ARRAY:
          depth++;
          if (depth <= 1) {
            putArray(dst, (BsonArray) value, tree, prefix);
          } else {
            putArrayAsDoc(dst, (BsonArray) value, tree, prefix);
          }
          depth--;
          break;
      }
    }

    private void putDocument(ResultRow dst, BsonDocument doc, PathTree tree, List<String> prefix) {
      for (Map.Entry<String, BsonValue> field : doc.entrySet()) {
        String node = field.getKey();
        BsonValue value = field.getValue();

        PathTree subTree = new PathTree();
        if (tree.getChildren().containsKey(null)) {
          subTree.put(null, tree);
          subTree.put(tree.getChildren().get(null));
        }
        if (tree.getChildren().containsKey(node)) {
          subTree.put(tree.getChildren().get(node));
        }

        put(dst, value, subTree, prefix, node);
      }
    }

    private void putArray(ResultRow dst, BsonArray arr, PathTree tree, List<String> prefix) {
      List<ResultRow> rowList = new ArrayList<>();
      List<String> subPrefix = new ArrayList<>();
      for (BsonValue value : arr) {
        ResultRow row = new ResultRow();
        put(row, value, tree, subPrefix);
        rowList.add(row);
      }
      List<String> pathPrefix = new ArrayList<>(prefix);
      dst.add(pathPrefix, rowList);
    }

    private void putArrayAsDoc(ResultRow dst, BsonArray arr, PathTree tree, List<String> prefix) {
      if (!tree.getChildren().containsKey(null)) {
        for (Map.Entry<String, PathTree> child : tree.getChildren().entrySet()) {
          String node = child.getKey();
          PathTree subTree = child.getValue();

          Integer index = cn.edu.tsinghua.iginx.mongodb.dummy.TypeUtils.parseInt(node);
          if (index != null && 0 <= index && index < arr.size()) {
            BsonValue value = arr.get(index);
            put(dst, value, subTree, prefix, node);
          }
        }
      } else {
        PathTree wildcardChild = tree.getChildren().get(null);
        for (int idx = 0; idx < arr.size(); idx++) {
          String node = TypeUtils.toString(idx);
          BsonValue value = arr.get(idx);

          PathTree subTree = new PathTree();
          subTree.put(null, wildcardChild);
          subTree.put(wildcardChild);
          if (tree.getChildren().containsKey(node)) {
            subTree.put(tree.getChildren().get(node));
          }

          put(dst, value, subTree, prefix, node);
        }
      }
    }

    private void put(
        ResultRow dst, BsonValue value, PathTree tree, List<String> prefix, String node) {
      prefix.add(node);
      put(dst, value, tree, prefix);
      prefix.remove(prefix.size() - 1);
    }
  }
}
