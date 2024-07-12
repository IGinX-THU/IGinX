/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.mongodb.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.TypeUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import java.util.*;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

public class JoinQuery {
  private final MongoDatabase db;

  public JoinQuery(MongoDatabase db) {
    this.db = db;
  }

  public RowStream query(List<Field> fieldList, Filter filter) {
    Field mainField = null;
    Map<Field, String> renamedFields = new HashMap<>();
    for (Field field : fieldList) {
      if (renamedFields.isEmpty()) {
        mainField = field;
        renamedFields.put(field, MongoDBStorage.VALUE_FIELD);
      } else {
        renamedFields.put(field, String.valueOf(renamedFields.size()));
      }
    }

    if (mainField == null) {
      return new EmptyRowStream();
    }

    Bson preFilter = FilterUtils.getPreFilter(filter);
    Bson postFilter = FilterUtils.getPostFilter(filter, renamedFields);
    List<Bson> mainStageList = getMainStageList(preFilter, renamedFields, postFilter);

    String mainCollName = NameUtils.getCollectionName(mainField);
    MongoCollection<BsonDocument> coll = this.db.getCollection(mainCollName, BsonDocument.class);
    MongoIterable<BsonDocument> result = coll.aggregate(mainStageList);

    return new QueryRowStream(result.cursor(), renamedFields);
  }

  private static List<Bson> getMainStageList(
      Bson preFilter, Map<Field, String> renamedFields, Bson postFilter) {
    List<Bson> mainStageList = new ArrayList<>();

    Bson preMatch = Aggregates.match(preFilter);
    mainStageList.add(preMatch);

    List<Bson> joinStageList = getJoinStageList(preFilter, renamedFields);
    mainStageList.addAll(joinStageList);

    if (postFilter != null) {
      Bson postMatch = Aggregates.match(postFilter);
      mainStageList.add(postMatch);
    }

    Bson sort = Aggregates.sort(new Document("_id", 1));
    mainStageList.add(sort);

    return mainStageList;
  }

  private static List<Bson> getJoinStageList(Bson preFilter, Map<Field, String> renamedFields) {
    List<Bson> stageList = new ArrayList<>();

    for (Map.Entry<Field, String> renamedField : renamedFields.entrySet()) {
      List<Bson> subStageList = new ArrayList<>();

      Bson preMatch = Aggregates.match(preFilter);
      subStageList.add(preMatch);

      String newName = renamedField.getValue();
      Bson rename = Aggregates.project(new Document(newName, "$" + MongoDBStorage.VALUE_FIELD));
      subStageList.add(rename);

      Field field = renamedField.getKey();
      String collName = NameUtils.getCollectionName(field);
      Bson union =
          new Document(
              "$unionWith", new Document("coll", collName).append("pipeline", subStageList));

      stageList.add(union);
    }

    if (!stageList.isEmpty()) {
      Bson group =
          Aggregates.group("$_id", new BsonField("row", new Document("$mergeObjects", "$$ROOT")));
      stageList.add(group);

      Bson replace = Aggregates.replaceRoot("$row");
      stageList.add(replace);
    }

    return stageList;
  }

  private static class EmptyRowStream implements RowStream {

    @Override
    public Header getHeader() {
      return new Header(Field.KEY, Collections.emptyList());
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Row next() {
      throw new NoSuchElementException();
    }
  }

  private static class QueryRowStream implements RowStream {

    private final MongoCursor<BsonDocument> cursor;

    private final Header header;

    private final Map<String, Integer> nameIdx;

    public QueryRowStream(MongoCursor<BsonDocument> cursor, Map<Field, String> renamedFields) {
      this.cursor = cursor;
      this.nameIdx = new HashMap<>();
      List<Field> fieldList = new ArrayList<>();
      for (Map.Entry<Field, String> renamedField : renamedFields.entrySet()) {
        this.nameIdx.put(renamedField.getValue(), this.nameIdx.size());
        fieldList.add(renamedField.getKey());
      }
      this.header = new Header(Field.KEY, fieldList);
    }

    @Override
    public Header getHeader() {
      return this.header;
    }

    @Override
    public void close() throws PhysicalException {
      try {
        this.cursor.close();
      } catch (Exception e) {
        throw new PhysicalException(e);
      }
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      try {
        return this.cursor.hasNext();
      } catch (Exception e) {
        throw new PhysicalException(e);
      }
    }

    @Override
    public Row next() throws PhysicalException {
      try {
        BsonDocument doc = this.cursor.next();

        long key = doc.remove("_id").asInt64().getValue();

        Object[] data = new Object[header.getFieldSize()];
        for (Map.Entry<String, BsonValue> renamedValue : doc.entrySet()) {
          int idx = this.nameIdx.get(renamedValue.getKey());
          Object value = TypeUtils.toObject(renamedValue.getValue());
          data[idx] = value;
        }

        return new Row(this.header, key, data);
      } catch (Exception e) {
        throw new PhysicalException(e);
      }
    }
  }
}
