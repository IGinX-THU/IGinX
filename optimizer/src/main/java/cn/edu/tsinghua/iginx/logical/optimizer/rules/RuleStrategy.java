package cn.edu.tsinghua.iginx.logical.optimizer.rules;

public enum RuleStrategy {
  FIXED_POINT, // 一直执行直到不再有匹配
  ONCE // 只执行一次
}
