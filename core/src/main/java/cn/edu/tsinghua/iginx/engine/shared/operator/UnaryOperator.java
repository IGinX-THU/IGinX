package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public interface UnaryOperator extends Operator {

  Source getSource();

  UnaryOperator copyWithSource(Source source);

  void setSource(Source source);
}
