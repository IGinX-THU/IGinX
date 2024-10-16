package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;
import java.util.Collections;

public abstract class BinaryFunction<OUT extends FieldVector> extends AbstractFunction {

  protected BinaryFunction(String name) {
    super(name, Arity.UNARY);
  }

  @Override
  protected VectorSchemaRoot invokeImpl(ExecutorContext context, VectorSchemaRoot args) throws ComputeException {
    FieldVector leftVector = args.getVector(0);
    FieldVector rightVector = args.getVector(1);
    FieldVector resultVector = evaluate(context, leftVector, rightVector);
    return new VectorSchemaRoot(Collections.singleton(resultVector));
  }

  @Override
  public boolean allowType(int index, Types.MinorType type) {
    switch (index) {
      case 0:
        return allowLeftType(type);
      case 1:
        return allowRightType(type);
      default:
        return false;
    }
  }

  protected abstract boolean allowLeftType(Types.MinorType type);

  protected abstract boolean allowRightType(Types.MinorType type);

  public abstract OUT evaluate(ExecutorContext context, @WillNotClose FieldVector left, @WillNotClose FieldVector right) throws ComputeException;
}
