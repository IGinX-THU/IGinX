package cn.edu.tsinghua.iginx.engine.distributedquery.coordinator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface Evaluator {

  boolean needDistributedQuery(Operator root);
}
