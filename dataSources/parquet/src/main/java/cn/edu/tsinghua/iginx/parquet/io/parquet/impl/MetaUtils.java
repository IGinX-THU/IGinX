package cn.edu.tsinghua.iginx.parquet.io.parquet.impl;

import cn.edu.tsinghua.iginx.parquet.io.FormatException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.*;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

public class MetaUtils {
    public static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    public static final int SIZE_OF_FOOTER_LENGTH = 4;

    public static FileMetaData read(SeekableByteChannel channel, ParquetReadOptions options) throws IOException, FormatException {
        ParquetFileReader.readFooter(options, channel, channel.size());
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
