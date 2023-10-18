package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.bson.*;
import org.bson.types.Decimal128;

class TypeUtils {

  private static final ThreadLocal<Map<String, Integer>> parseIntCache = new ThreadLocal<>();
  private static final ThreadLocal<Map<Integer, String>> encodeIntCache = new ThreadLocal<>();

  public static Integer parseInt(String s) {
    Function<String, Integer> converter =
        str -> {
          try {
            return Integer.parseInt(str);
          } catch (Exception e) {
            return null;
          }
        };
    if (parseIntCache.get() == null) {
      parseIntCache.set(new HashMap<>());
    }
    return parseIntCache.get().computeIfAbsent(s, converter);
  }

  public static String toString(int i) {
    if (encodeIntCache.get() == null) {
      encodeIntCache.set(new HashMap<>());
    }
    return encodeIntCache.get().computeIfAbsent(i, String::valueOf);
  }

  public static DataType convert(BsonType type) {
    switch (type) {
      case DOUBLE:
        return DataType.DOUBLE;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case DATE_TIME:
      case TIMESTAMP:
      case INT64:
        return DataType.LONG;
      case STRING:
      case SYMBOL:
      default:
        return DataType.BINARY;
    }
  }

  private static final String MAGIK_STR = "$";

  public static Object convert(BsonValue value) {
    switch (value.getBsonType()) {
      case DOUBLE:
        return ((BsonDouble) value).getValue();
      case BOOLEAN:
        return ((BsonBoolean) value).getValue();
      case DATE_TIME:
        return ((BsonDateTime) value).getValue();
      case INT32:
        return ((BsonInt32) value).getValue();
      case TIMESTAMP:
        return ((BsonTimestamp) value).getValue();
      case INT64:
        return ((BsonInt64) value).getValue();
      case STRING:
        return ((BsonString) value).getValue().getBytes();
      case SYMBOL:
        return ((BsonSymbol) value).getSymbol().getBytes();
    }
    return convertToBinary(value);
  }

  public static BsonValue convert(Value value) {
    switch (value.getDataType()) {
      case BOOLEAN:
        return new BsonBoolean(value.getBoolV());
      case INTEGER:
        return new BsonInt32(value.getIntV());
      case LONG:
        return new BsonInt64(value.getLongV());
      case FLOAT:
        return new BsonDouble(value.getFloatV());
      case DOUBLE:
        return new BsonDouble(value.getDoubleV());
      case BINARY:
        {
          try {
            BsonDocument doc = BsonDocument.parse(value.getBinaryVAsString());
            BsonValue v = doc.get(MAGIK_STR);
            if (v == null) {
              v = new BsonString(value.getBinaryVAsString());
            }
            return v;
          } catch (Exception ignored) {
            return new BsonString(value.getBinaryVAsString());
          }
        }
      default:
        throw new IllegalArgumentException("unsupported value:" + value);
    }
  }

  public static byte[] convertToBinary(BsonValue value) {
    BsonDocument doc = new BsonDocument(MAGIK_STR, value);
    return doc.toJson().getBytes();
  }

  public static BsonValue convertTo(BsonValue value, BsonType type) {
    if (value.getBsonType().equals(type)) {
      return value;
    }
    switch (type) {
      case UNDEFINED:
        return new BsonUndefined();
      case NULL:
        return new BsonNull();
      case MIN_KEY:
        return new BsonMinKey();
      case MAX_KEY:
        return new BsonMaxKey();
    }
    switch (value.getBsonType()) {
      case REGULAR_EXPRESSION:
        return convertTo(((BsonRegularExpression) value).getPattern(), type);
      case STRING:
        return convertTo(((BsonString) value).getValue(), type);
      case SYMBOL:
        return convertTo(((BsonSymbol) value).getSymbol(), type);
      case NULL:
        return null;
      case BOOLEAN:
        return convertTo(((BsonBoolean) value).getValue() ? 1 : 0, type);
      case DATE_TIME:
        return convertTo(((BsonDateTime) value).getValue(), type);
      case INT32:
        return convertTo(((BsonInt32) value).getValue(), type);
      case TIMESTAMP:
        return convertTo(((BsonTimestamp) value).getValue(), type);
      case INT64:
        return convertTo(((BsonInt64) value).getValue(), type);
      case DOUBLE:
        return convertTo(((BsonDouble) value).getValue(), type);
      case DECIMAL128:
        return convertTo(((BsonDecimal128) value).getValue().doubleValue(), type);
    }
    throw new IllegalArgumentException("can't convert " + value + " to " + type);
  }

  private static BsonValue convertTo(String s, BsonType type) {
    if (s.isEmpty() || s.equals("null")) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        if (s.equalsIgnoreCase("true")) {
          return new BsonBoolean(true);
        } else if (s.equalsIgnoreCase("false")) {
          return new BsonBoolean(false);
        }
        break;
      case INT32:
        return new BsonInt32((int) convertToTimestamp(s));
      case INT64:
        return new BsonInt64(convertToTimestamp(s));
      case DATE_TIME:
        return new BsonDateTime(convertToTimestamp(s));
      case TIMESTAMP:
        return new BsonTimestamp(convertToTimestamp(s));
      case DOUBLE:
        return new BsonDouble(Double.parseDouble(s));
      case DECIMAL128:
        return new BsonDecimal128(Decimal128.parse(s));
      case STRING:
        return new BsonString(s);
      case REGULAR_EXPRESSION:
        return new BsonRegularExpression(s);
      case SYMBOL:
        return new BsonSymbol(s);
      case BINARY:
        return new BsonBinary(s.getBytes());
    }
    throw new IllegalArgumentException("can't convert \"" + s + "\" to " + type);
  }

  private static long convertToTimestamp(String s) {
    try {
      return DateFormat.getDateTimeInstance().parse(s).getTime();
    } catch (Exception e) {
      return Long.parseLong(s);
    }
  }

  private static BsonValue convertTo(long number, BsonType type) {
    switch (type) {
      case BOOLEAN:
        return new BsonBoolean(number != 0);
      case INT32:
        return new BsonInt32((int) number);
      case INT64:
        return new BsonInt64(number);
      case DATE_TIME:
        return new BsonDateTime(number);
      case TIMESTAMP:
        return new BsonTimestamp(number);
      case DOUBLE:
        return new BsonDouble((double) number);
      case DECIMAL128:
        return new BsonDecimal128(new Decimal128(BigDecimal.valueOf(number)));
      case STRING:
        return new BsonString(String.valueOf(number));
      case REGULAR_EXPRESSION:
        return new BsonRegularExpression(String.valueOf(number));
      case SYMBOL:
        return new BsonSymbol(String.valueOf(number));
    }
    throw new IllegalArgumentException("can't convert " + number + " to " + type);
  }

  private static BsonValue convertTo(double number, BsonType type) {
    switch (type) {
      case BOOLEAN:
        return new BsonBoolean(number != 0);
      case INT32:
        return new BsonInt32((int) number);
      case INT64:
        return new BsonInt64((long) number);
      case DOUBLE:
        return new BsonDouble(number);
      case DECIMAL128:
        return new BsonDecimal128(new Decimal128(BigDecimal.valueOf(number)));
      case STRING:
        return new BsonString(String.valueOf(number));
      case REGULAR_EXPRESSION:
        return new BsonRegularExpression(String.valueOf(number));
      case SYMBOL:
        return new BsonSymbol(String.valueOf(number));
    }
    throw new IllegalArgumentException("can't convert " + number + " to " + type);
  }
}
