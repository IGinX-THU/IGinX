package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.io.FileMeta;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.local.ParquetFileReader;
import org.apache.parquet.local.ParquetReadOptions;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class ParquetMeta implements FileMeta {

  public static final String PATH_DELIMITER = ".";

  ParquetMetadata metaData;

  final Path path;

  ParquetMeta(Path path, ParquetReadOptions options) throws IOException {
    this.path = path;
    InputFile input = new LocalInputFile(path);
    try (SeekableInputStream stream = input.newStream()) {
      this.metaData = ParquetFileReader.readFooter(input, options, stream);
    }
  }

  @Nullable
  @Override
  public Set<String> fields() {
    Set<String> result = new HashSet<>();
    for (String[] path : metaData.getFileMetaData().getSchema().getPaths()) {
      result.add(String.join(PATH_DELIMITER, path));
    }
    return result;
  }

  @Nullable
  @Override
  public DataType getType(@Nonnull String field) {
    String[] path = StringUtils.split(field, PATH_DELIMITER);
    Type type = metaData.getFileMetaData().getSchema().getType(path);
    if (!type.isPrimitive()) {
      return null;
    }
    return toIginxType(type.asPrimitiveType());
  }

  @Nullable
  @Override
  public Map<String, String> extra() {
    return metaData.getFileMetaData().getKeyValueMetaData();
  }

  public static DataType toIginxType(PrimitiveType primitiveType) {
    if (primitiveType.getRepetition().equals(PrimitiveType.Repetition.REPEATED)) {
      return DataType.BINARY;
    }
    switch (primitiveType.getPrimitiveTypeName()) {
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
        throw new RuntimeException(
            "Unsupported data type: " + primitiveType.getPrimitiveTypeName());
    }
  }
}
