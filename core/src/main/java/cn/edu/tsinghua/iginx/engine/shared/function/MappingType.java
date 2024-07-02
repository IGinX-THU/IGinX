package cn.edu.tsinghua.iginx.engine.shared.function;

public enum MappingType {
  Mapping, // 输入是一个行的集合，输出也是一个行的集合
  SetMapping, // 输入是一个行的集合，输出是一行
  RowMapping // 输入是一行，输出也是一行
}
