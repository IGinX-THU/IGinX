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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import com.google.common.collect.ImmutableSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.vector.FieldVector;
import org.apache.commons.lang3.stream.Streams;

public class WriteBatch implements NoexceptAutoCloseable, Iterable<MemBatch.Snapshot> {

  private final Iterable<MemBatch.Snapshot> batches;

  WriteBatch(@WillCloseWhenClosed Iterable<MemBatch.Snapshot> batches) {
    this.batches = Objects.requireNonNull(batches);
  }

  public Set<Field> getFields() {
    return Streams.of(batches)
        .flatMap(b -> b.getFieldVectors().stream())
        .map(FieldVector::getField)
        .map(ArrowFields::toIginxField)
        .collect(ImmutableSet.toImmutableSet());
  }

  @Override
  public void close() {
    batches.forEach(MemBatch.Snapshot::close);
  }

  @Override
  public Iterator<MemBatch.Snapshot> iterator() {
    return batches.iterator();
  }
}
