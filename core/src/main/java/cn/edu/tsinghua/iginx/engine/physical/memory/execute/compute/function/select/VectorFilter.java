package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.select;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.AbstractFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;

import java.util.Collections;

public class VectorFilter extends AbstractFunction {

  public static final String NAME = "filter";

  public VectorFilter() {
    super(NAME, Arity.BINARY);
  }

  @Override
  protected boolean allowType(int index, Types.MinorType type) {
    return index == 0 && type == Types.MinorType.BIT;
  }

  public <T extends FieldVector> FieldVector evaluate(ExecutorContext context, BitVector bitVector, T fieldVector) {
    T result = ValueVectors.like(context.getAllocator(), fieldVector);
    int targetIndex = 0;
    for (int sourceIndex = 0; sourceIndex < bitVector.getValueCount(); sourceIndex++) {
      if (!bitVector.isNull(sourceIndex) && bitVector.get(sourceIndex) != 0) {
        result.copyFrom(sourceIndex, targetIndex, fieldVector);
        targetIndex++;
      }
    }
    result.setValueCount(targetIndex);
    return result;
  }

  @Override
  protected VectorSchemaRoot invokeImpl(ExecutorContext context, VectorSchemaRoot args) throws ComputeException {
    FieldVector bitVector = args.getFieldVectors().get(0);
    FieldVector fieldVector = args.getFieldVectors().get(1);
    FieldVector result = evaluate(context, (BitVector) bitVector, fieldVector);
    return new VectorSchemaRoot(Collections.singletonList(result));
  }

  @Override
  public void close() {

  }
}
