package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.TypeUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.*;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

public class ColumnQuery {
  private final MongoDatabase database;

  public ColumnQuery(MongoDatabase database) {
    this.database = database;
  }

  public RowStream query(List<Field> fieldList, KeyInterval range) {
    Bson columnFilter = FilterUtils.interval(range);
    QueryRowStream.Builder builder = QueryRowStream.builder(fieldList.size());
    for (Field field : fieldList) {
      builder.add(field);
      String collName = NameUtils.getCollectionName(field);
      MongoCollection<BsonDocument> coll =
          this.database.getCollection(collName, BsonDocument.class);
      for (BsonDocument document : coll.find(columnFilter)) {
        long key = document.get("_id").asInt64().getValue();
        Object value = TypeUtils.toObject(document.get(MongoDBStorage.VALUE_FIELD));
        builder.put(key, value);
      }
    }
    return builder.build();
  }

  public static class QueryRowStream implements RowStream {

    private final Header header;

    private final Iterator<Map.Entry<Long, Object[]>> rowItr;

    public QueryRowStream(Header header, Iterator<Map.Entry<Long, Object[]>> rowItr) {
      this.header = header;
      this.rowItr = rowItr;
    }

    @Override
    public Header getHeader() {
      return this.header;
    }

    @Override
    public void close() {
      while (this.rowItr.hasNext()) {
        this.rowItr.next();
      }
    }

    @Override
    public boolean hasNext() {
      return this.rowItr.hasNext();
    }

    @Override
    public Row next() {
      Map.Entry<Long, Object[]> rowData = this.rowItr.next();
      return new Row(this.header, rowData.getKey(), rowData.getValue());
    }

    public static Builder builder(int columnNum) {
      return new Builder(columnNum);
    }

    public static class Builder {

      private final int columnNum;

      private final List<Field> fieldList;

      private final SortedMap<Long, Object[]> rows;

      private Builder(int columnNum) {
        this.columnNum = columnNum;
        this.fieldList = new ArrayList<>(columnNum);
        this.rows = new TreeMap<>();
      }

      public void add(Field field) {
        this.fieldList.add(field);
      }

      public void put(Long key, Object value) {
        int idx = fieldList.size() - 1;
        Object[] row = rows.computeIfAbsent(key, k -> new Object[this.columnNum]);
        row[idx] = value;
      }

      public QueryRowStream build() {
        Header header = new Header(Field.KEY, new ArrayList<>(this.fieldList));
        return new QueryRowStream(header, rows.entrySet().iterator());
      }
    }
  }
}
