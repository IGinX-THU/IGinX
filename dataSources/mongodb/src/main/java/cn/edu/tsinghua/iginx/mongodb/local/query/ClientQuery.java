package cn.edu.tsinghua.iginx.mongodb.local.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.mongodb.local.entity.PathTree;
import cn.edu.tsinghua.iginx.mongodb.local.entity.ResultTable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.*;

public class ClientQuery {

  private final MongoClient client;

  public ClientQuery(MongoClient client) {
    this.client = client;
  }

  public RowStream query(PathTree pathTree, Filter filter) {
    Map<String, PathTree> trees = getDatabaseTrees(pathTree);
    List<ResultTable> resultList = new ArrayList<>();
    for (Map.Entry<String, PathTree> tree : trees.entrySet()) {
      MongoDatabase db = this.client.getDatabase(tree.getKey());
      List<ResultTable> dbResultList = new DatabaseQuery(db).query(tree.getValue());
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

  private RowStream joinByKeyOn(List<ResultTable> tables, Filter condition) {
    return new QueryRowStream(tables, condition);
  }

  private static class QueryRowStream implements RowStream {

    private final Header header;

    private final Iterator<Long> keyItr;

    private final Map<Integer, ResultTable> tables = new TreeMap<>();

    private final Filter condition;

    public QueryRowStream(List<ResultTable> results, Filter condition) {
      List<Field> fields = new ArrayList<>();
      SortedSet<Long> keys = new TreeSet<>();
      for (ResultTable result : results) {
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

      return nextRow == null;
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
        for (Map.Entry<Integer, ResultTable> offsetTable : tables.entrySet()) {
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
}
