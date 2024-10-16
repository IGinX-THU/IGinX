package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;

import javax.annotation.WillNotClose;
import java.util.Collections;

public abstract class UnaryFunction<OUT extends FieldVector> extends AbstractFunction {

  protected UnaryFunction(String name) {
    super(name, Arity.UNARY);
  }

  @Override
  protected VectorSchemaRoot invokeImpl(ExecutorContext context, VectorSchemaRoot args) throws ComputeException {
    FieldVector resultVector = evaluate(context, args.getVector(0));
    return new VectorSchemaRoot(Collections.singleton(resultVector));
  }

  @Override
  public boolean allowType(int index, Types.MinorType type) {
    if (index == 0) {
      return allowType(type);
    }
    return false;
  }

  protected abstract boolean allowType(Types.MinorType type);

  public abstract OUT evaluate(ExecutorContext context, @WillNotClose FieldVector input) throws ComputeException;
}
