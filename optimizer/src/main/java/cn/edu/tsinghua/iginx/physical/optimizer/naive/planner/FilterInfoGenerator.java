package cn.edu.tsinghua.iginx.physical.optimizer.naive.planner;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.ScalarFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare.GreaterEqual;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare.Less;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.compare.LessEqual;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.CallNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.FieldNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.LiteralNode;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression.PhysicalExpression;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.And;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.logic.Or;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorInitializer;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class FilterInfoGenerator implements UnaryExecutorInitializer<PhysicalExpression> {

  private final Filter filter;

  public FilterInfoGenerator(Filter filter) {
    this.filter = Objects.requireNonNull(filter);
  }

  @Override
  public PhysicalExpression initialize(ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return construct(filter, context, inputSchema);
  }

  private static PhysicalExpression construct(Filter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    switch (filter.getType()) {
      case Value:
        return construct((ValueFilter) filter, context, inputSchema);
      case And:
        return construct((AndFilter) filter, context, inputSchema);
      case Or:
        return construct((OrFilter) filter, context, inputSchema);
      default:
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getType());
    }
  }

  private static PhysicalExpression construct(AndFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return and(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static PhysicalExpression construct(OrFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    return or(construct(filter.getChildren(), context, inputSchema), context, inputSchema);
  }

  private static List<PhysicalExpression> construct(List<Filter> filters, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    List<PhysicalExpression> result = new ArrayList<>();
    try {
      for (Filter filter : filters) {
        result.add(construct(filter, context, inputSchema));
      }
    } catch (ComputeException e) {
      result.forEach(PhysicalExpression::close);
      throw e;
    }
    return result;
  }

  private static PhysicalExpression and(List<PhysicalExpression> children, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(true), context, inputSchema);
    }
    return children.stream()
        .reduce((left, right) -> new CallNode(new And(), left, right))
        .orElseThrow(() -> new IllegalStateException("Empty children list"));
  }

  private static PhysicalExpression or(List<PhysicalExpression> children, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    if (children.isEmpty()) {
      return construct(new BoolFilter(false), context, inputSchema);
    }
    return children.stream()
        .reduce((left, right) -> new CallNode(new Or(), left, right))
        .orElseThrow(() -> new IllegalStateException("Empty children list"));
  }

  private static PhysicalExpression construct(ValueFilter filter, ExecutorContext context, BatchSchema inputSchema) throws ComputeException {
    List<Integer> paths = match(filter.getPath(), context, inputSchema);
    List<PhysicalExpression> comparisons = new ArrayList<>();
    for (Integer pathIndex : paths) {
      comparisons.add(new CallNode(getPredicate(filter.getOp()), new FieldNode(pathIndex), new LiteralNode(filter.getValue().getValue())));
    }
    if (Op.isOrOp(filter.getOp())) {
      return or(comparisons, context, inputSchema);
    } else if (Op.isAndOp(filter.getOp())) {
      return and(comparisons, context, inputSchema);
    } else {
      throw new UnsupportedOperationException("Unsupported operator: " + filter.getOp());
    }
  }

  private static ScalarFunction getPredicate(Op op) {
    switch (op) {
      case GE:
      case GE_AND:
        return new GreaterEqual();
      case LE:
      case LE_AND:
        return new LessEqual();
      case L:
      case L_AND:
        return new Less();
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + op);
    }
  }

  private static List<Integer> match(String path, ExecutorContext context, BatchSchema inputSchema) {
    Predicate<String> matcher = StringUtils.toColumnMatcher(path);
    List<Integer> result = new ArrayList<>();
    for (int i = 0; i < inputSchema.getFieldCount(); i++) {
      if (matcher.test(inputSchema.getField(i).getName())) {
        result.add(i);
      }
    }
    return result;
  }

}
