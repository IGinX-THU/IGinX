package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.query.entity.MongoDBSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.bson.Document;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

public class DataUtils {

    public static String reformatPattern(String pattern) {
        pattern = pattern.replaceAll("[.]", "\\\\.");
        pattern = pattern.replaceAll("[*]", ".*");
        return pattern;
    }

    public static String toString(DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return "int";
            case LONG:
                return "long";
            case BOOLEAN:
                return "bool";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case BINARY:
                return "binary";
        }
        return "";
    }

    public static DataType fromString(String dataType) {
        switch (dataType) {
            case "bool":
                return BOOLEAN;
            case "int":
                return INTEGER;
            case "long":
                return LONG;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "binary":
                return BINARY;
            default:
                throw new UnsupportedDataTypeException(dataType);
        }
    }

    public static Document constructDocument(MongoDBSchema schema, DataType type, Object value, long timestamp) {
        Document document = new Document(MongoDBStorage.ID, String.format(MongoDBStorage.ID_TEMPLATE, timestamp, schema.getName()));
        document.append(MongoDBStorage.NAME, schema.getName()).append(MongoDBStorage.TS, timestamp);
        document.append(MongoDBStorage.TYPE, toString(type)).append(MongoDBStorage.VALUE, value);
        document.append(MongoDBStorage.TAGS, schema.getTagsAsString());
        return document;
    }

}
