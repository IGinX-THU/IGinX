/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.minio.tree.format;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.minio.common.IginxPaths;
import cn.edu.tsinghua.iginx.minio.common.Patterns;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.ParquetReadOptions;
import shaded.iginx.org.apache.parquet.hadoop.ParquetFileReader;
import shaded.iginx.org.apache.parquet.hadoop.ParquetRecordReader;
import shaded.iginx.org.apache.parquet.hadoop.metadata.BlockMetaData;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.InputFile;
import shaded.iginx.org.apache.parquet.io.SeekableInputStream;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.Types;

/**
 * Parquet objects with primitive columns. A repeated column is read as BINARY, which concatenates
 * big-endian bytes of its values.
 */
final class ParquetFormat implements ObjectFormat {

  static final String NAME = "Parquet";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<String> getExtensions() {
    return Collections.singletonList("parquet");
  }

  @Override
  public Reader newReader(@Nullable String prefix, Path path, Config config) throws IOException {
    InputFile file = new NioInputFile(path);
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    ParquetMetadata footer;
    try (SeekableInputStream in = file.newStream()) {
      footer = ParquetFileReader.readFooter(file, options, in);
    }
    return new ParquetReader(prefix, file, footer, options);
  }

  @Override
  public String toString() {
    return NAME;
  }

  static DataType toIginxType(PrimitiveType type) {
    if (type.isRepetition(Type.Repetition.REPEATED)) {
      return DataType.BINARY;
    }
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
        throw new IllegalArgumentException(
            "unsupported parquet type: " + type.getPrimitiveTypeName());
    }
  }

  private static final class ParquetReader implements Reader {

    private final InputFile file;
    private final ParquetMetadata footer;
    private final ParquetReadOptions options;
    private final MessageType schema;
    private final Map<String, DataType> types = new HashMap<>();
    private final Map<String, String> fieldToRaw = new HashMap<>();
    private final Map<String, String> rawToField = new HashMap<>();

    ParquetReader(
        @Nullable String prefix,
        InputFile file,
        ParquetMetadata footer,
        ParquetReadOptions options) {
      this.file = file;
      this.footer = footer;
      this.options = options;
      this.schema = footer.getFileMetaData().getSchema();
      for (Type type : schema.getFields()) {
        if (!type.isPrimitive()) {
          throw new IllegalArgumentException("unsupported parquet type: " + type);
        }
        String raw = type.getName();
        String field = IginxPaths.join(prefix, raw);
        types.put(field, toIginxType(type.asPrimitiveType()));
        fieldToRaw.put(field, raw);
        rawToField.put(raw, field);
      }
    }

    @Override
    public Map<String, DataType> find(Collection<String> patterns) {
      Map<String, DataType> result = new HashMap<>();
      for (String field : types.keySet()) {
        if (Patterns.match(patterns, field)) {
          result.put(field, types.get(field));
        }
      }
      return result;
    }

    @Override
    public RowStream read(List<String> fields, RangeSet<Long> keys) throws IOException {
      Set<String> raws = new HashSet<>();
      for (String field : fields) {
        raws.add(fieldToRaw.get(field));
      }
      MessageType requested = project(raws);

      List<Field> headerFields = new ArrayList<>();
      for (Type type : requested.getFields()) {
        headerFields.add(
            new Field(rawToField.get(type.getName()), toIginxType(type.asPrimitiveType())));
      }
      Header header = new Header(Field.KEY, headerFields);

      long rows = footer.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
      ParquetFileReader fileReader = new ParquetFileReader(file, footer, options);
      try {
        fileReader.setRequestedSchema(requested);
        ParquetRecordReader<Object[]> recordReader =
            new ParquetRecordReader<>(new RowMaterializer(requested), fileReader, options);
        return new ParquetRowStream(header, recordReader, new KeyCursor(keys, rows));
      } catch (IOException | RuntimeException e) {
        fileReader.close();
        throw e;
      }
    }

    private MessageType project(Set<String> raws) {
      Types.MessageTypeBuilder builder = Types.buildMessage();
      for (String raw : new HashSet<>(raws)) {
        if (schema.containsField(raw)) {
          builder.addField(schema.getType(raw));
        }
      }
      return builder.named(schema.getName());
    }
  }

  private static final class ParquetRowStream implements RowStream {

    private final Header header;
    private final ParquetRecordReader<Object[]> reader;
    private final KeyCursor cursor;
    private Row next;

    ParquetRowStream(Header header, ParquetRecordReader<Object[]> reader, KeyCursor cursor)
        throws IOException {
      this.header = header;
      this.reader = reader;
      this.cursor = cursor;
      this.next = fetch();
    }

    @Nullable
    private Row fetch() throws IOException {
      while (reader.nextKeyValue()) {
        long key = reader.getCurrentRowIndex();
        if (cursor.exhausted(key)) {
          return null;
        }
        if (cursor.contains(key)) {
          return new Row(header, key, reader.getCurrentValue());
        }
      }
      return null;
    }

    @Override
    public Header getHeader() {
      return header;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Row next() throws PhysicalException {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Row row = next;
      try {
        next = fetch();
      } catch (IOException e) {
        throw new PhysicalException("failed to read parquet", e);
      }
      return row;
    }

    @Override
    public void close() throws PhysicalException {
      try {
        reader.close();
      } catch (IOException e) {
        throw new PhysicalException(e);
      }
    }
  }
}
