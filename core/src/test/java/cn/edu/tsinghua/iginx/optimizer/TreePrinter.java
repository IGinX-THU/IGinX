package cn.edu.tsinghua.iginx.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.TreeInfoVisitor;

public class TreePrinter {

  public static String getTreeInfo(Operator root) {
    TreeInfoVisitor visitor = new TreeInfoVisitor();
    root.accept(visitor);
    return visitor.getTreeInfo();
  }
}
