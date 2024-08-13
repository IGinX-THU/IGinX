package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class ParquetFormatReader implements FileFormat.Reader {

  private final IParquetReader.Builder builder;
  private final ParquetMetadata footer;
  private final String prefix;

  public ParquetFormatReader(IParquetReader.Builder builder, ParquetMetadata footer, @Nullable String prefix) {
    this.builder = Objects.requireNonNull(builder);
    this.prefix = prefix;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Map<String, DataType> find(Collection<String> patterns) throws IOException {
    return
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    return null;
  }


}
