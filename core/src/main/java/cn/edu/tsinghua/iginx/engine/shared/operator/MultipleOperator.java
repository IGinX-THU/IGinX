package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.List;

public interface MultipleOperator extends Operator {

  List<Source> getSources();

  MultipleOperator copyWithSource(List<Source> sources);

  void setSources(List<Source> sources);
}
