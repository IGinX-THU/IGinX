package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.ScalarExpressions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.hash.Hash;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.ComparableRowCursor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;
import java.util.*;

public class JoinHashMap implements AutoCloseable {

  private final BufferAllocator allocator;
  private final int batchSize;
  private final List<VectorSchemaRoot> stagedKeysList;
  private final List<VectorSchemaRoot> stagedValuesList;
  private final Hash hasher;
  private final ListMultimap<Integer, JoinCursor> map;

  private final Schema probeSideSchema;
  private final List<ScalarExpression<?>> probeSideKeyExpressions;
  private final List<ScalarExpression<?>> probeSideValueExpressions;

  private final Queue<VectorSchemaRoot> stagedResults;

  private final VectorSchemaRoot buffer;
  private final RowCursor buildSideOutputCursor;
  private final RowCursor probeSideOutputCursor;
  private final Optional<BitVector> markColumn;
  private int bufferedRowCount = 0;

  protected JoinHashMap(
      BufferAllocator allocator,
      int batchSize,
      List<VectorSchemaRoot> stagedKeysList,
      List<VectorSchemaRoot> stagedValuesList,
      Hash hasher,
      ListMultimap<Integer, JoinCursor> map,
      Schema buildSideValuesSchema,
      Schema probeSideSchema,
      List<ScalarExpression<?>> probeSideKeyExpressions,
      List<ScalarExpression<?>> probeSideValueExpressions,
      @Nullable
      String markColumnName,
      @Nullable
      ScalarExpression<BitVector> filterExpression
  ) {
    this.allocator = allocator;
    this.batchSize = batchSize;
    this.hasher = hasher;
    this.map = map;
    this.stagedKeysList = stagedKeysList;
    this.stagedValuesList = stagedValuesList;

    this.probeSideSchema = probeSideSchema;
    this.probeSideKeyExpressions = probeSideKeyExpressions;
    this.probeSideValueExpressions = probeSideValueExpressions;

    VectorSchemaRoot buildSideBuffer = VectorSchemaRoot.create(buildSideValuesSchema, allocator);
    VectorSchemaRoot probeSideBuffer = VectorSchemaRoot.create(probeSideSchema, allocator);
    this.buildSideOutputCursor = new RowCursor(buildSideBuffer);
    this.probeSideOutputCursor = new RowCursor(probeSideBuffer);
    if (markColumnName != null) {
      BitVector markColumn = new BitVector(markColumnName, allocator);
      this.markColumn = Optional.of(markColumn);
    } else {
      this.markColumn = Optional.empty();
    }

    List<FieldVector> allBuffers = new ArrayList<>();
    allBuffers.addAll(buildSideBuffer.getFieldVectors());
    allBuffers.addAll(probeSideBuffer.getFieldVectors());
    this.markColumn.ifPresent(allBuffers::add);
    this.buffer = new VectorSchemaRoot(allBuffers);

    this.stagedResults = new ArrayDeque<>();
  }

  @Override
  public void close() throws Exception {
    stagedKeysList.forEach(VectorSchemaRoot::close);
    stagedValuesList.forEach(VectorSchemaRoot::close);
    stagedResults.forEach(VectorSchemaRoot::close);
    buffer.close();
  }

  public boolean hasStagedResult() {
    return stagedResults.isEmpty();
  }

  public VectorSchemaRoot pollStagedResult() {
    return stagedResults.remove();
  }

  public void flush(boolean dumpUntouched) throws ComputeException {
    for (Map.Entry<ComparableRowCursor, JoinGroup> entry : map.entrySet()) {
      if (!entry.getValue().isTouched()) {
        for (RowCursor valuesCursor : entry.getValue()) {

        }
      }
    }
  }

  public void probe(VectorSchemaRoot batch, boolean yieldNotFounded) throws ComputeException {
    Preconditions.checkArgument(Objects.equals(batch.getSchema(), probeSideSchema), "Batch schema does not match probe side schema");

    VectorSchemaRoot keys = ScalarExpressions.evaluateSafe(allocator, stagedKeysList, batch);
    ComparableRowCursor keysCursorTemplate = new ComparableRowCursor(keys);
    RowCursor outputCursorTemplate = new RowCursor(outputCursor.getColumns(), 0);
    for (int rowIndex = 0; rowIndex < batch.getRowCount(); rowIndex++) {
      ComparableRowCursor keysCursor = keysCursorTemplate.withPosition(rowIndex);
      RowCursor outputCursorRow = outputCursorTemplate.withPosition(rowIndex);

      JoinGroup joinGroup = map.get(keysCursor);
      if (joinGroup != null) {
        joinGroup.touch();
        for (RowCursor valuesCursor : joinGroup) {
          outputCursorRow.copyFrom(valuesCursor);
        }
      }
    }
  }


  public static class Builder implements AutoCloseable {

    private final BufferAllocator allocator;
    private final Schema buildSideSchema;
    private final List<ScalarExpression<?>> buildSideKeyExpressions;
    private final List<ScalarExpression<?>> buildSideValueExpressions;
    private final List<VectorSchemaRoot> stagedKeysList;
    private final List<VectorSchemaRoot> stagedValuesList;
    private final Hash hasher = new Hash();
    private final ListMultimap<Integer, JoinCursor> map;
    private boolean closed = false;

    public Builder(BufferAllocator allocator, Schema buildSideSchema, List<ScalarExpression<?>> buildSideKeyExpressions, List<ScalarExpression<?>> buildSideValueExpressions) {
      this.allocator = Objects.requireNonNull(allocator);
      this.map = MultimapBuilder.hashKeys().arrayListValues().build();
      this.buildSideSchema = Objects.requireNonNull(buildSideSchema);
      this.buildSideKeyExpressions = new ArrayList<>(buildSideKeyExpressions);
      this.buildSideValueExpressions = new ArrayList<>(buildSideValueExpressions);
      this.stagedKeysList = new ArrayList<>();
      this.stagedValuesList = new ArrayList<>();
    }

    public Builder add(BufferAllocator allocator, VectorSchemaRoot batch) throws ComputeException {
      Preconditions.checkState(!closed, "Builder is closed");
      Preconditions.checkArgument(Objects.equals(batch.getSchema(), buildSideSchema), "Batch schema does not match build side schema");

      VectorSchemaRoot keys = ScalarExpressions.evaluateSafe(allocator, buildSideKeyExpressions, batch);
      stagedKeysList.add(keys);

      VectorSchemaRoot values = ScalarExpressions.evaluateSafe(allocator, buildSideValueExpressions, batch);
      stagedValuesList.add(values);

      JoinCursor cursorTemplate = new JoinCursor(keys, values);
      try (IntVector hashCodes = hasher.invoke(allocator, keys)) {
        for (int rowIndex = 0; rowIndex < batch.getRowCount(); rowIndex++) {
          map.put(hashCodes.get(rowIndex), cursorTemplate.withPosition(rowIndex));
        }
      }

      return this;
    }

    public JoinHashMap build(
        int batchSize,
        Schema probeSideSchema,
        List<ScalarExpression<?>> probeSideKeyExpressions,
        List<ScalarExpression<?>> probeSideValueExpressions,
        @Nullable String markColumnName
    ) throws ComputeException {
      Preconditions.checkState(!closed, "Builder is closed");
      Preconditions.checkNotNull(allocator, "Allocator is required");
      Preconditions.checkNotNull(probeSideSchema, "Probe side schema is required");
      Preconditions.checkNotNull(probeSideKeyExpressions, "Build side key expressions are required");
      Preconditions.checkNotNull(probeSideValueExpressions, "Build side value expressions are required");
      closed = true;

      Schema probeKeySchema = ScalarExpressions.getOutputSchema(allocator, probeSideKeyExpressions, probeSideSchema);
      Schema buildSideKeySchema = ScalarExpressions.getOutputSchema(allocator, buildSideKeyExpressions, buildSideSchema);
      Preconditions.checkArgument(Objects.equals(probeKeySchema, buildSideKeySchema), "Probe key schema does not match build key schema");

      Schema buildSideValuesSchema = ScalarExpressions.getOutputSchema(allocator, buildSideValueExpressions, buildSideSchema);
      return new JoinHashMap(
          allocator,
          batchSize,
          stagedKeysList,
          stagedValuesList,
          map,
          buildSideValuesSchema,
          markColumnName,
          probeSideSchema,
          probeSideKeyExpressions,
          probeSideValueExpressions
      );
    }

    @Override
    public void close() throws Exception {
      if (!closed) {
        stagedKeysList.forEach(VectorSchemaRoot::close);
        stagedValuesList.forEach(VectorSchemaRoot::close);
        closed = true;
      }
    }
  }

  protected static class JoinCursor extends RowCursor {

    private final FieldVector[] valueColumns;
    private boolean matched = false;

    public JoinCursor(VectorSchemaRoot keys, VectorSchemaRoot values) {
      this(keys.getFieldVectors().toArray(new FieldVector[0]), values.getFieldVectors().toArray(new FieldVector[0]), 0);
    }

    protected JoinCursor(FieldVector[] keyColumns, FieldVector[] valueColumns, int index) {
      super(keyColumns, index);
      this.valueColumns = valueColumns;
    }

    public JoinCursor withPosition(int position) {
      return new JoinCursor(this.columns, this.valueColumns, position);
    }

    @Override
    public void setPosition(int position) {
      throw new UnsupportedOperationException("JoinCursor does not support setPosition");
    }

    public boolean isMatched() {
      return matched;
    }

    public void setMatched() {
      matched = true;
    }
  }

}
