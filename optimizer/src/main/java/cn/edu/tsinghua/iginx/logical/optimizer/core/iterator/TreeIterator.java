package cn.edu.tsinghua.iginx.logical.optimizer.core.iterator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.Iterator;

public interface TreeIterator extends Iterator<Operator> {

  boolean hasNext();

  Operator next();
}
