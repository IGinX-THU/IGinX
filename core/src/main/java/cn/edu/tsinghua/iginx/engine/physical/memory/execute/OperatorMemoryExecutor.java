package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public interface OperatorMemoryExecutor {

  RowStream executeUnaryOperator(UnaryOperator operator, RowStream stream, RequestContext context)
      throws PhysicalException;

  RowStream executeBinaryOperator(
      BinaryOperator operator, RowStream streamA, RowStream streamB, RequestContext context)
      throws PhysicalException;
}
