package cn.edu.tsinghua.iginx.engine.physical.task.visitor;

import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;

public interface TaskVisitor {

  /**
   * do sth when you enter a task, this method will be called at the beginning of the 'accept'
   * method.
   */
  default void enter() {}

  /** do sth when you leave a task, this method will be called at the end of 'accept' method. */
  default void leave() {}

  void visit(BinaryMemoryPhysicalTask task);

  void visit(UnaryMemoryPhysicalTask task);

  void visit(MultipleMemoryPhysicalTask task);

  void visit(StoragePhysicalTask task);

  void visit(GlobalPhysicalTask task);
}
