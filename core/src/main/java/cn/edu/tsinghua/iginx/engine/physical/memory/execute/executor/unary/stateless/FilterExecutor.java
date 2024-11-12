package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.expression.PredicateExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Objects;

public class FilterExecutor extends StatelessUnaryExecutor {

  private final PredicateExpression condition;

  public FilterExecutor(ExecutorContext context, Schema inputSchema, PredicateExpression condition) {
    super(context, inputSchema);
    this.condition = Objects.requireNonNull(condition);
  }

  @Override
  public Batch compute(Batch batch) throws ComputeException {
    BaseIntVector selection = condition.filter(context.getAllocator(), batch.getDictionaryProvider(), batch.getData(), batch.getSelection());
    return batch.sliceWith(context.getAllocator(), selection);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  @Override
  protected String getInfo() {
    return "Filter by " + condition.getName();
  }

  @Override
  public void close() throws ComputeException {

  }
}
