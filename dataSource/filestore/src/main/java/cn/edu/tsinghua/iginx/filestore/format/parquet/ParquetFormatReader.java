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
    Set<Field> allSchema = new HashSet<>();

    List<Path> filePaths = getFilePaths();
    filePaths.sort(Comparator.naturalOrder());
    for (Path path : filePaths) {
      IParquetReader.Builder builder = IParquetReader.builder(path);
      try (IParquetReader reader = builder.build()) {
        ParquetRecordIterator iterator = new ParquetRecordIterator(reader, prefix);
        List<Field> fields = iterator.header();
        allSchema.addAll(fields);
      } catch (Exception e) {
        throw new PhysicalRuntimeException("failed to load schema from " + path, e);
      }
    }

    List<Column> columns = new ArrayList<>();
    for (Field field : allSchema) {
      columns.add(new Column(field.getName(), field.getType(), field.getTags(), true));
    }
    return columns;
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    return null;
  }


  private static IParquetReader readFile(
      Path path,
      List<String> prefix,
      List<String> patterns,
      TagFilter tagFilter,
      RangeSet<Long> ranges,
      Map<String, DataType> declaredTypes) throws IOException {

    AtomicBoolean isIginxData = new AtomicBoolean(false);
    IParquetReader.Builder builder = IParquetReader.builder(path);
    builder.withSchemaConverter((messageType, extra) -> {
      ParquetSchema parquetSchema;
      try {
        parquetSchema = new ParquetSchema(messageType, extra, prefix);
      } catch (UnsupportedTypeException e) {
        throw new StorageRuntimeException(e);
      }

      isIginxData.set(parquetSchema.getKeyIndex() != null);
      List<Pair<String, DataType>> rawHeader = parquetSchema.getRawHeader();
      List<String> rawNames = rawHeader.stream().map(Pair::getK).collect(Collectors.toList());
      List<DataType> rawTypes = rawHeader.stream().map(Pair::getV).collect(Collectors.toList());
      List<Field> header = parquetSchema.getHeader();

      Set<String> projectedPath = new HashSet<>();
      int size = rawNames.size();
      for (int i = 0; i < size; i++) {
        if (!rawTypes.get(i).equals(declaredTypes.get(header.get(i).getFullName()))) {
          continue;
        }

        Field field = header.get(i);
        if (parquetSchema.getKeyIndex() != null && tagFilter != null) {
          if (!TagKVUtils.match(field.getTags(), tagFilter)) {
            continue;
          }
        }

        if (!patterns.stream().anyMatch(s -> StringUtils.match(field.getName(), s))) {
          continue;
        }

        projectedPath.add(rawNames.get(i));
      }

      if (isIginxData.get()) {
        projectedPath.add(Constants.KEY_FIELD_NAME);
      }

      return IParquetReader.project(messageType, projectedPath);
    });

    if (isIginxData.get()) {
      builder.filter(FilterRangeUtils.filterOf(ranges));
    } else {
      if (!ranges.isEmpty()) {
        builder.range(0L, 0L);
      }
      KeyInterval keyInterval = RangeUtils.toKeyInterval(ranges.span());
      builder.range(keyInterval.getStartKey(), keyInterval.getEndKey());
    }

    return builder.build();
  }


}
