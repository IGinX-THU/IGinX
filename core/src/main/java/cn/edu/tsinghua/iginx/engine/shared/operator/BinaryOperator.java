package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public interface BinaryOperator extends Operator {

  Source getSourceA();

  Source getSourceB();

  BinaryOperator copyWithSource(Source sourceA, Source sourceB);

  void setSourceA(Source source);

  void setSourceB(Source source);
}
