package cn.edu.tsinghua.iginx.physical.optimizer.naive.planner;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;

import java.util.List;
import java.util.Objects;

public class TagKVProjectionInfoGenerator implements UnaryExecutorInitializer<List<PhysicalExpression>> {

  private final TagFilter tagFilter;

  public TagKVProjectionInfoGenerator(TagFilter tagFilter) {
    this.tagFilter = Objects.requireNonNull(tagFilter);
  }

  @Override
  public List<PhysicalExpression> initialize(ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    throw new ComputeException("TagFilter not implemented");
  }
}
