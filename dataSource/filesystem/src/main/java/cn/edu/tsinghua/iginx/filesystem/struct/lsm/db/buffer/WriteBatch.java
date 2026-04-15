package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.FieldVector;
import org.apache.commons.lang3.stream.Streams;

import javax.annotation.WillCloseWhenClosed;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

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
