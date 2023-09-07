package cn.edu.tsinghua.iginx.mongodb.immigrant.entity;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.fields;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Projections;
import java.util.*;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

public class Query {

  private final List<Bson> stages = new ArrayList<>();

  @Override
  public String toString() {
    return "Immigrant{" + "stages=" + stages + '}';
  }

  public RowStream query(MongoCollection<BsonDocument> collection) {
    return Results.buildTable(collection.aggregate(stages)).rowStream();
  }

  public Query last() {
    this.stages.add(Aggregates.project(fields(exclude("_id." + MongoId.KEY_SUBFIELD))));
    this.stages.add(group("$_id", new BsonField("last", new Document("$mergeObjects", "$$ROOT"))));
    this.stages.add(replaceRoot("$last"));
    return this;
  }

  public void range(KeyInterval range) {
    this.stages.add(match(FilterUtils.interval(range)));
  }

  public void filter(TagFilter tagFilter) {
    this.stages.add(match(FilterUtils.withTags(tagFilter)));
  }

  public void filter(Filter filter) {
    this.stages.add(match(FilterUtils.match(filter)));
  }

  public void project(List<String> patterns) {
    if (patterns.stream().noneMatch(NameUtils::isWildcardAll)) {
      if (patterns.stream().noneMatch(NameUtils::isWildcard)) {
        this.normalProject(patterns);
      } else {
        this.wildcardProject(patterns);
      }
    }
  }

  private void normalProject(List<String> paths) {
    List<Bson> fieldList =
        paths.stream()
            .map(NameUtils::encodePath)
            .map(Projections::include)
            .collect(Collectors.toList());
    this.stages.add(Aggregates.project(fields(fieldList)));
  }

  private void wildcardProject(List<String> pathPatterns) {
    List<Bson> fieldRegexList =
        pathPatterns.stream()
            .map(NameUtils::encodePath)
            .map(pattern -> Utils.regexMatch("$$this.k", pattern))
            .collect(Collectors.toList());
    this.stages.add(Utils.filterRoot(new Document("$or", fieldRegexList)));
    this.stages.add(match(nor(FilterUtils.nonFieldExcept("$$ROOT", "_id"))));
  }

  public static class Utils {
    public static Bson filterRoot(Object arrayFilterCond) {
      Bson originArray = new Document("$objectToArray", "$$ROOT");
      Bson filteredArray =
          new Document(
              "$filter", new Document("input", originArray).append("cond", arrayFilterCond));
      Bson filteredRoot = new Document("$arrayToObject", filteredArray);
      Bson mergedRoot =
          new Document("$mergeObjects", Arrays.asList(filteredRoot, new Document("_id", "$_id")));
      return replaceRoot(mergedRoot);
    }

    public static Bson regexMatch(Object input, String regex) {
      return new Document("$regexMatch", new Document("input", input).append("regex", regex + "$"));
    }

    public static Bson unsetField(Object input, Object field) {
      return new Document("$unsetField", new Document("field", field).append("input", input));
    }

    public static Bson filter(Object input, Object cond) {
      return new Document("$filter", new Document("input", input).append("cond", cond));
    }
  }

  private static class Results {

    public static Result buildTable(MongoIterable<BsonDocument> results) {
      Result.Builder builder = new Result.Builder();
      try (MongoCursor<BsonDocument> cursor = results.cursor()) {
        while (cursor.hasNext()) {
          builder.append(buildRow(cursor.next()));
        }
      }
      return builder.build();
    }

    private static MongoRow buildRow(BsonDocument doc) {
      MongoId id = buildId(doc.getDocument("_id"));
      Map<String, MongoPoint> fields = new TreeMap<>();
      for (Map.Entry<String, BsonValue> field : doc.entrySet()) {
        if (!field.getKey().equals("_id")) {
          String path = NameUtils.decodePath(field.getKey());
          MongoPoint point = MongoPoint.of(field.getValue());
          fields.put(path, point);
        }
      }
      return new MongoRow(id, fields);
    }

    private static MongoId buildId(BsonDocument doc) {
      long key = doc.getInt64(MongoId.KEY_SUBFIELD, new BsonInt64(0)).getValue();
      Map<String, String> tags = new TreeMap<>();
      for (Map.Entry<String, BsonValue> tag : doc.entrySet()) {
        if (!tag.getKey().equals(MongoId.KEY_SUBFIELD)) {
          String tagK = NameUtils.decodeTagK(tag.getKey());
          String tagV = tag.getValue().asString().getValue();
          tags.put(tagK, tagV);
        }
      }

      return new MongoId(key, tags);
    }
  }

  public static class Result {

    private final Header header;
    private final Iterable<Row> rows;

    private Result(Header header, Iterable<Row> rows) {
      this.header = header;
      this.rows = rows;
    }

    public RowStream rowStream() {
      return new RowStream() {
        private final Iterator<Row> itr = Result.this.rows.iterator();
        private final Header header = Result.this.header;

        @Override
        public Header getHeader() {
          return header;
        }

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
          return this.itr.hasNext();
        }

        @Override
        public Row next() {
          return this.itr.next();
        }
      };
    }

    public static class Builder {
      private final TreeMap<Long, ArrayList<Object>> valueTable = new TreeMap<>();
      private final HashMap<String, Integer> fieldIndexes = new HashMap<>();
      private final HashMap<String, Field> fields = new HashMap<>();

      public void append(MongoRow row) {
        MongoId id = row.getId();
        for (Map.Entry<String, MongoPoint> mongoField : row.getFields().entrySet()) {
          MongoPoint point = mongoField.getValue();
          Field field = new Field(mongoField.getKey(), point.getType(), id.getTags());
          int idx =
              fieldIndexes.computeIfAbsent(
                  field.getFullName(),
                  k -> {
                    fields.put(field.getFullName(), field);
                    return fieldIndexes.size();
                  });
          List<Object> values =
              valueTable.computeIfAbsent(id.getKey(), k -> new ArrayList<>(fieldIndexes.size()));
          ensureSizeList(values, idx + 1).set(idx, point.getValue());
        }
      }

      public Result build() {
        List<Field> fieldList = ensureSizeList(new ArrayList<>(), fields.size());
        for (Map.Entry<String, Integer> fieldIndex : this.fieldIndexes.entrySet()) {
          fieldList.set(fieldIndex.getValue(), fields.get(fieldIndex.getKey()));
        }
        Header header = new Header(Field.KEY, fieldList);

        List<Row> rowList = new ArrayList<>(valueTable.size());
        for (Map.Entry<Long, ArrayList<Object>> valueRow : valueTable.entrySet()) {
          List<Object> valueList = ensureSizeList(valueRow.getValue(), header.getFieldSize());
          if (valueRow.getKey() != MongoId.PLACE_HOLDER_KEY) {
            rowList.add(new Row(header, valueRow.getKey(), valueList.toArray()));
          }
        }

        return new Result(header, rowList);
      }

      private static <E> List<E> ensureSizeList(List<E> list, int size) {
        while (list.size() < size) {
          list.add(null);
        }
        return list;
      }
    }
  }
}
