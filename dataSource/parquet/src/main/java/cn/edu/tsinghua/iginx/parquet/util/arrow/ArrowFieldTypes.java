package cn.edu.tsinghua.iginx.parquet.util.arrow;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ArrowFieldTypes {

  public static FieldType of(
      boolean nullable,
      DataType dataType,
      @Nullable DictionaryEncoding dictionary,
      @Nullable Map<String, String> metadata) {
    Preconditions.checkNotNull(dataType, "dataType");

    return new FieldType(nullable, ArrowTypes.of(dataType), dictionary, metadata);
  }

  public static FieldType nonnull(DataType dataType, @Nullable Map<String, String> metadata) {
    return of(false, dataType, null, null);
  }

  public static FieldType nonnull(DataType dataType) {
    return nonnull(dataType, null);
  }
}
