package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.IginxPaths;
import cn.edu.tsinghua.iginx.filestore.common.Patterns;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.MessageType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class ParquetFormatReader implements FileFormat.Reader {

  private final IParquetReader.Builder builder;
  private final ParquetMetadata footer;
  private final Map<String, DataType> fields = new HashMap<>();
  private final Map<String, String> fieldToRawName = new HashMap<>();
  private final Map<String, String> rawNameToField = new HashMap<>();

  public ParquetFormatReader(IParquetReader.Builder builder, ParquetMetadata footer, @Nullable String prefix) throws IOException {
    this.builder = Objects.requireNonNull(builder);
    this.footer = Objects.requireNonNull(footer);
    initSchema(footer.getFileMetaData().getSchema(), prefix);
  }

  private void initSchema(MessageType schema, String prefix) throws IOException {
    List<Field> fields = ProjectUtils.toFields(schema);
    for (Field field : fields) {
      String rawName = field.getName();
      String fullName = IginxPaths.join(prefix, rawName);
      this.fields.put(fullName, field.getType());
      this.fieldToRawName.put(fullName, rawName);
      this.rawNameToField.put(rawName, fullName);
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Map<String, DataType> find(Collection<String> patterns) throws IOException {
    Map<String, DataType> result = new HashMap<>();
    for (String field : fields.keySet()) {
      if (Patterns.match(patterns, field)) {
        result.put(field, fields.get(field));
      }
    }
    return result;
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    Set<String> rawFields = new HashSet<>();
    for (String field : fields) {
      rawFields.add(fieldToRawName.get(field));
    }

    IParquetReader reader = builder
        .project(rawFields, false)
        .build(footer);

    RowStream rowStream = new ParquetFormatRowStream(reader, rawNameToField::get);
    return RowStreams.filtered(rowStream, filter);
  }

}
