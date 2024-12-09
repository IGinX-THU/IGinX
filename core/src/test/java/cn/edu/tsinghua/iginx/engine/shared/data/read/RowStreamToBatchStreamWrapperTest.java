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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RowStreamToBatchStreamWrapperTest {

  @ParameterizedTest
  @CsvSource({
    "true,1984,0",
    "true,1984,1",
    "true,1984,2",
    "true,1984,1983",
    "true,1984,1984",
    "true,1984,1985",
    "true,1984,4096",
    "true,2048,0",
    "true,2048,1",
    "true,2048,2",
    "true,2048,2047",
    "true,2048,2048",
    "true,2048,2049",
    "true,2048,4096",
    "false,1984,0",
    "false,1984,1",
    "false,1984,2",
    "false,1984,1983",
    "false,1984,1984",
    "false,1984,1985",
    "false,1984,4096",
    "false,2048,0",
    "false,2048,1",
    "false,2048,2",
    "false,2048,2047",
    "false,2048,2048",
    "false,2048,2049",
    "false,2048,4096"
  })
  void test(boolean hasKey, int batchSize, int scale) throws PhysicalException {
    Field key = hasKey ? Field.KEY : null;
    Header header =
        new Header(
            key,
            Arrays.asList(
                new Field("booleanValue", DataType.BOOLEAN),
                new Field("integerValue", DataType.INTEGER),
                new Field("longValue", DataType.LONG),
                new Field("floatValue", DataType.FLOAT),
                new Field("doubleValue", DataType.DOUBLE),
                new Field("binaryValue", DataType.BINARY)));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < scale; i++) {
      Object[] values =
          new Object[] {
            i % 3 == 0 ? null : i % 2 == 0,
            i % 5 == 0 ? null : i,
            i % 7 == 0 ? null : (long) i,
            i % 11 == 0 ? null : (float) i,
            i % 13 == 0 ? null : (double) i,
            i % 17 == 0 ? null : RandomStringUtils.random(10).getBytes()
          };
      if (hasKey) {
        rows.add(new Row(header, i, values));
      } else {
        rows.add(new Row(header, values));
      }
    }
    RowStream rowStream = new Table(header, rows);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      try (BatchStream batchStream = BatchStreams.wrap(allocator, rowStream, batchSize)) {
        // check schema
        BatchSchema.Builder schemaBuilder = BatchSchema.builder();
        if (hasKey) {
          schemaBuilder.withKey();
        }
        schemaBuilder
            .addField("booleanValue", DataType.BOOLEAN)
            .addField("integerValue", DataType.INTEGER)
            .addField("longValue", DataType.LONG)
            .addField("floatValue", DataType.FLOAT)
            .addField("doubleValue", DataType.DOUBLE)
            .addField("binaryValue", DataType.BINARY)
            .build();
        Assertions.assertEquals(schemaBuilder.build(), batchStream.getSchema());

        // check data
        for (int batchIndex = 0; batchIndex * batchSize < scale; batchIndex++) {
          try (Batch batch = batchStream.getNext()) {
            Assertions.assertNotNull(batch);
            Assertions.assertEquals(
                Math.min(batchSize, scale - batchIndex * batchSize), batch.getRowCount());
            try (org.apache.arrow.vector.table.Table table =
                new org.apache.arrow.vector.table.Table(batch.getData())) {
              org.apache.arrow.vector.table.Row arrowRow = table.immutableRow();
              for (int rowIndex = 0; rowIndex < batch.getRowCount(); rowIndex++) {
                int globalRowIndex = batchIndex * batchSize + rowIndex;
                Row iginxRow = rows.get(globalRowIndex);
                arrowRow.setPosition(rowIndex);
                if (hasKey) {
                  Assertions.assertEquals(iginxRow.getKey(), arrowRow.getBigInt(0));
                }
                Object[] values = iginxRow.getValues();
                int columnOffset = hasKey ? 1 : 0;
                Assertions.assertEquals(
                    values[0],
                    arrowRow.isNull(0 + columnOffset)
                        ? null
                        : arrowRow.getBit(0 + columnOffset) != 0);
                Assertions.assertEquals(
                    values[1],
                    arrowRow.isNull(1 + columnOffset) ? null : arrowRow.getInt(1 + columnOffset));
                Assertions.assertEquals(
                    values[2],
                    arrowRow.isNull(2 + columnOffset)
                        ? null
                        : arrowRow.getBigInt(2 + columnOffset));
                Assertions.assertEquals(
                    values[3],
                    arrowRow.isNull(3 + columnOffset)
                        ? null
                        : arrowRow.getFloat4(3 + columnOffset));
                Assertions.assertEquals(
                    values[4],
                    arrowRow.isNull(4 + columnOffset)
                        ? null
                        : arrowRow.getFloat8(4 + columnOffset));
                Assertions.assertArrayEquals(
                    (byte[]) values[5],
                    arrowRow.isNull(5 + columnOffset)
                        ? null
                        : arrowRow.getVarBinary(5 + columnOffset));
              }
            }
          }
        }
        Assertions.assertFalse(batchStream.hasNext());
      }
    }
  }
}
