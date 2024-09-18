package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.ExecutorType;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.pipeline.PipelineExecutor;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public class NaivePhysicalExecutorFactory {

  public ExecutorType getExecutorType(Operator operator) {
    switch (operator.getType()) {
      case Project:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:
        return ExecutorType.Pipeline;
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }

  PipelineExecutor createPipelineExecutor(UnaryOperator operator) {
    switch (operator.getType()) {
      case Project:
      case Rename:
      case Reorder:
      case AddSchemaPrefix:
        throw new UnsupportedOperationException("Not implemented yet");
        //        return new Projector();
      default:
        throw new UnsupportedOperationException("Unsupported operator type: " + operator.getType());
    }
  }
}
