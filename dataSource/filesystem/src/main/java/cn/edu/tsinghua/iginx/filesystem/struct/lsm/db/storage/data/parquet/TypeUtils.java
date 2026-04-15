package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.TagKVUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.arrow.util.Preconditions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.shade.org.apache.parquet.column.statistics.LongStatistics;
import org.apache.paimon.shade.org.apache.parquet.column.statistics.Statistics;
import org.apache.paimon.shade.org.apache.parquet.schema.PrimitiveType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.HashMap;
import java.util.Map;

public class TypeUtils {

  public static RowType toRowType(Header header) {
    RowType.Builder builder = RowType.builder();
    Field key = Field.KEY;
    builder.field(TagKVUtils.toFullName(key.getName(), key.getTags()), new BigIntType(false));
    for (Field field : header.getFields()) {
      String name = TagKVUtils.toFullName(field.getName(), field.getTags());
      org.apache.paimon.types.DataType type = toPaimonDataType(field.getType());
      builder.field(name, type);
    }
    return builder.build();
  }

  public static org.apache.paimon.types.DataType toPaimonDataType(DataType type) {
    switch (type) {
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case INTEGER:
        return DataTypes.INT();
      case LONG:
        return DataTypes.BIGINT();
      case FLOAT:
        return DataTypes.FLOAT();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case BINARY:
        return DataTypes.STRING();
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static InternalRow toInternalRow(Row row) {
    Object[] values = row.getValues();
    GenericRow genericRow = new GenericRow(values.length + 1);
    genericRow.setField(0, row.getKey());
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      if(value instanceof byte[]) {
        value = BinaryString.fromBytes((byte[]) value);
      }
      genericRow.setField(i + 1,value);
    }
    return genericRow;
  }

  public static ImmutableMap<Field, Table.Statistic> toStatisticMap(Map<String, Statistics<?>> left) {
    Map<Field, Statistics<?>> fieldStats = new HashMap<>();
    for (Map.Entry<String, Statistics<?>> entry : left.entrySet()) {
      String name = entry.getKey();
      Statistics<?> statistics = entry.getValue();
      ColumnKey columnKey = TagKVUtils.splitFullName(name);
      cn.edu.tsinghua.iginx.thrift.DataType type = toIginxType(statistics.type());

      Field field = new Field(columnKey.getPath(), type, columnKey.getTags());
      fieldStats.put(field, statistics);
    }

    LongStatistics keyStats = (LongStatistics) fieldStats.remove(Field.KEY);
    Preconditions.checkArgument(keyStats != null, "Key statistics is missing");

    Range<Long> keyRange;
    if (keyStats.hasNonNullValue()) {
      keyRange = Range.closed(keyStats.getMin(), keyStats.getMax());
    } else {
      keyRange = Range.closedOpen(0L, 0L);
    }
    Table.Statistic statistic = new Table.Statistic(keyRange);

    return fieldStats.keySet().stream().collect(ImmutableMap.toImmutableMap(f -> f, f -> statistic));
  }

  public static DataType toIginxType(PrimitiveType type) {
    Preconditions.checkArgument(type.isPrimitive());
    switch (type.getPrimitiveTypeName()) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BINARY:
        return DataType.BINARY;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  public static Row toRow(InternalRow paimonRow, Header header, InternalRow.FieldGetter[] fieldGetters) {
    long key = paimonRow.getLong(0);
    Object[] values = new Object[header.getFields().size()];
    for (int i = 0; i < values.length; i++) {
      Object value = fieldGetters[i + 1].getFieldOrNull(paimonRow);
      if(value instanceof BinaryString) {
        value = ((BinaryString) value).toBytes();
      }
      values[i] = value;
    }
    return new Row(header, key, values);
  }
}
