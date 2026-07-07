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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.WriteBatches;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.DenseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowFormat extends DenseImmutableFileFormat {

  private final ArrowConfig arrowConfig;

  public ArrowFormat(Config config, CachePool cachePool, Indexer indexer) {
    super("arrow", ArrowConfig.of(config), cachePool);
    this.arrowConfig = (ArrowConfig) this.config;
  }

  @Override
  protected void flush(Path dst, Table.SubTable subTable) throws IOException, PhysicalException {
    try (RowStream rowStream = scanAll(subTable)) {
      Header header = rowStream.getHeader();
      org.apache.arrow.vector.types.pojo.Field arrowKeyField = ArrowFields.of(Field.KEY);
      List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();
      arrowFields.add(arrowKeyField);
      arrowFields.addAll(ArrowFields.of(header.getFields()));

      try (BufferAllocator allocator = new RootAllocator();
          VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(arrowFields), allocator);
          WritableByteChannel channel =
              Files.newByteChannel(dst, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
          ArrowFileWriter writer =
              new ArrowFileWriter(
                  root,
                  null,
                  channel,
                  null,
                  IpcOption.DEFAULT,
                  FastestCompressionFactory.INSTANCE,
                  arrowConfig.getCompression(),
                  Optional.ofNullable(arrowConfig.getCompressionLevel()))) {
        writer.start();
        while (rowStream.hasNext()) {
          List<Row> rows = new ArrayList<>();
          for (int i = 0;
              i < BaseFixedWidthVector.INITIAL_VALUE_ALLOCATION && rowStream.hasNext();
              i++) {
            rows.add(rowStream.next());
          }
          writeRowsIntoRoot(root, rows);
          writer.writeBatch();
        }
        writer.end();
      }
    }
    flushMeta(getMetaPath(dst), subTable.getMeta());
  }

  private static void writeRowsIntoRoot(VectorSchemaRoot root, List<Row> rows) {
    root.clear();
    WriteBatches.fillVector(root.getVector(0), idx -> rows.get(idx).getKey(), rows.size());
    for (int colIdx = 1; colIdx < root.getFieldVectors().size(); colIdx++) {
      FieldVector vector = root.getVector(colIdx);
      int rowColIdx = colIdx - 1;
      WriteBatches.fillVector(vector, idx -> rows.get(idx).getValue(rowColIdx), rows.size());
    }
    root.setRowCount(rows.size());
  }

  private Path getMetaPath(Path path) {
    return path.resolveSibling(path.getFileName().toString() + ".meta");
  }

  private void flushMeta(Path path, Table.Meta meta) throws IOException {
    try (ObjectOutputStream oos =
        new ObjectOutputStream(
            Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
      oos.writeObject(meta);
    }
  }

  @Override
  protected Table.Meta loadMeta(Path src) throws IOException {
    try (ObjectInputStream ois =
        new ObjectInputStream(Files.newInputStream(getMetaPath(src), StandardOpenOption.READ))) {
      return (Table.Meta) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to read meta file for " + src, e);
    }
  }

  @Override
  protected RowStream scan(Path src, List<Field> fields, Filter predicate) throws IOException {
    Header header = new Header(Field.KEY, fields);
    org.apache.arrow.vector.types.pojo.Field arrowKeyField = ArrowFields.of(Field.KEY);
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = ArrowFields.of(fields);

    List<Row> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        SeekableByteChannel channel = Files.newByteChannel(src, StandardOpenOption.READ);
        ArrowFileReader reader =
            new ArrowFileReader(channel, allocator, FastestCompressionFactory.INSTANCE)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        BigIntVector keyVector = (BigIntVector) root.getVector(arrowKeyField);
        FieldVector[] valueVectors = new FieldVector[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          org.apache.arrow.vector.types.pojo.Field field = arrowFields.get(i);
          FieldVector vector = root.getVector(field);
          if (vector == null) {
            throw new IOException("Field " + field + " does not exist in file " + src);
          }
          valueVectors[i] = vector;
        }

        int rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; i++) {
          long key = keyVector.get(i);
          Object[] values = new Object[valueVectors.length];
          for (int j = 0; j < valueVectors.length; j++) {
            values[j] = valueVectors[j].getObject(i);
          }
          rows.add(new Row(header, key, values));
        }
      }
    }

    RowStream result = new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    if (!Filters.isTrue(predicate)) {
      result = new FilterRowStreamWrapper(result, predicate);
    }
    return result;
  }
}
