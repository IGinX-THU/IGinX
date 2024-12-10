package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.function.udf;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.AbstractScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class UDFFunction extends AbstractScalarFunction<FieldVector> {

  private static Function function;

  private UDFType type;

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  protected UDFFunction(Function function) {
    super(function.getIdentifier(), new Arity(Arity.COMPLEX.getArity(), true));
    this.function = function;
    this.type = metaManager.getTransformTask(function.getIdentifier()).getType();
  }

  /**
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    return false;
  }

  /**
   * @param allocator
   * @param selection
   * @param input
   * @return
   * @throws ComputeException
   */
  @Override
  protected FieldVector invokeImpl(
      BufferAllocator allocator, @Nullable BaseIntVector selection, VectorSchemaRoot input)
      throws ComputeException {
    if (selection == null) {
      return invokeImpl(allocator, input);
    }
    try (VectorSchemaRoot selected = PhysicalFunctions.take(allocator, selection, input)) {
      return invokeImpl(allocator, selected);
    }
  }

  private FieldVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input) {
    return call(allocator, input);
  }

  protected abstract FieldVector call(BufferAllocator allocator, VectorSchemaRoot input);
}
