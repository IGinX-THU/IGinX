package cn.edu.tsinghua.iginx.mongodb.query.entity;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

public class MongoDBSchema {

    private final String name;

    private final Map<String, String> tags;

    private String tagString = null;

    private DataType type = null;

    public MongoDBSchema(String name, Map<String, String> tags, DataType type) {
        this.name = name;
        this.tags = tags;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getTagsAsString() {
        if (tagString != null) {
            return tagString;
        }
        if (tags == null || tags.isEmpty()) {
            return "";
        }

        TreeMap<String, String> tags = new TreeMap<>(this.tags);
        StringBuilder builder = new StringBuilder();
        int cnt = 0;
        for (String key: tags.keySet()) {
            if (cnt != 0) {
                builder.append(',');
            }
            builder.append(key);
            builder.append("=");
            builder.append(tags.get(key));
            cnt++;
        }
        tagString = builder.toString();
        return tagString;
    }

    public static Map<String, String> resolveTagsFromString(String tagString) {
        if (tagString == null || tagString.isEmpty()) {
            return null;
        }
        String[] partitions = tagString.split(",");
        Map<String, String> tags = new TreeMap<>();
        for (String partition: partitions) {
            String[] kAndV = partition.split("=");
            tags.put(kAndV[0], kAndV[1]);
        }
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MongoDBSchema that = (MongoDBSchema) o;
        return Objects.equals(name, that.name) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags);
    }


    public static boolean match(MongoDBSchema schema, TagFilter tagFilter) {
        return match(schema.getTags(), tagFilter);
    }

    public static boolean match(Map<String, String> tags, TagFilter tagFilter) {
        switch (tagFilter.getType()) {
            case And:
                return match(tags, (AndTagFilter) tagFilter);
            case Or:
                return match(tags, (OrTagFilter) tagFilter);
            case Base:
                return match(tags, (BaseTagFilter) tagFilter);
            case Precise:
                return match(tags, (PreciseTagFilter) tagFilter);
            case BasePrecise:
                return match(tags, (BasePreciseTagFilter) tagFilter);
            case WithoutTag:
                return match(tags, (WithoutTagFilter) tagFilter);
        }
        return false;
    }

    private static boolean match(Map<String, String> tags, AndTagFilter tagFilter) {
        if (tags == null || tags.isEmpty()) {
            return false;
        }
        List<TagFilter> children = tagFilter.getChildren();
        for (TagFilter child: children) {
            if (!match(tags, child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean match(Map<String, String> tags, OrTagFilter tagFilter) {
        if (tags == null || tags.isEmpty()) {
            return false;
        }
        List<TagFilter> children = tagFilter.getChildren();
        for (TagFilter child: children) {
            if (match(tags, child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean match(Map<String, String> tags, BaseTagFilter tagFilter) {
        if (tags == null || tags.isEmpty()) {
            return false;
        }
        String tagKey = tagFilter.getTagKey();
        String expectedValue = tagFilter.getTagValue();
        if (!tags.containsKey(tagKey)) {
            return false;
        }
        String actualValue = tags.get(tagKey);
        if (!StringUtils.isPattern(expectedValue)) {
            return expectedValue.equals(actualValue);
        } else {
            return Pattern.matches(StringUtils.reformatPath(expectedValue), actualValue);
        }
    }

    private static boolean match(Map<String, String> tags, PreciseTagFilter tagFilter) {
        if (tags == null || tags.isEmpty()) {
            return false;
        }
        List<BasePreciseTagFilter> children = tagFilter.getChildren();
        for (TagFilter child: children) {
            if (match(tags, child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean match(Map<String, String> tags, BasePreciseTagFilter tagFilter) {
        if (tags == null || tags.isEmpty()) {
            return false;
        }
        return tags.equals(tagFilter.getTags());
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    private static boolean match(Map<String, String> tags, WithoutTagFilter tagFilter) {
        return tags == null || tags.isEmpty();
    }

    public Field toField() {
        return new Field(name, type, tags);
    }

}
