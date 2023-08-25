package cn.edu.tsinghua.iginx.mongodb.query;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Projections.*;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.mongodb.entity.*;
import cn.edu.tsinghua.iginx.mongodb.tools.FilterUtils;
import cn.edu.tsinghua.iginx.mongodb.tools.NameUtils;
import com.mongodb.client.*;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Projections;
import java.util.*;
import java.util.stream.Collectors;
import org.bson.*;
import org.bson.conversions.Bson;

public class Query {

  private final List<Bson> stages = new ArrayList<>();

  @Override
  public String toString() {
    return stages.toString();
  }

  public IginxTable query(MongoCollection<BsonDocument> collection) {
    return Results.buildTable(collection.aggregate(stages));
  }

  public Query last() {
    this.stages.add(project(fields(exclude("_id." + MongoId.KEY_SUBFIELD))));
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

  public void normalProject(List<String> paths) {
    assert !paths.isEmpty();

    List<Bson> fieldList =
        paths.stream()
            .map(NameUtils::encodePath)
            .map(Projections::include)
            .collect(Collectors.toList());
    this.stages.add(project(fields(fieldList)));
  }

  public void wildcardProject(List<String> pathPatterns) {
    assert !pathPatterns.isEmpty();

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

    public static IginxTable buildTable(MongoIterable<BsonDocument> results) {
      IginxTable.Builder builder = new IginxTable.Builder();
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
}
