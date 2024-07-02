package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

public interface Filter {

  void accept(FilterVisitor visitor);

  FilterType getType();

  Filter copy();
}
