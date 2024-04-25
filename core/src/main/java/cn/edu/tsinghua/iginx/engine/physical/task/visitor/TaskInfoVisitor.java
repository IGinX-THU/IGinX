package cn.edu.tsinghua.iginx.engine.physical.task.visitor;

import cn.edu.tsinghua.iginx.engine.physical.task.*;
import java.util.ArrayList;
import java.util.List;

public class TaskInfoVisitor implements TaskVisitor {

  private final List<Object[]> cache = new ArrayList<>();

  private int maxLen = 0;

  private int depth = -1;

  public List<Object[]> getCache() {
    return cache;
  }

  public int getMaxLen() {
    return maxLen;
  }

  @Override
  public void enter() {
    depth++;
  }

  @Override
  public void leave() {
    depth--;
  }

  @Override
  public void visit(BinaryMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(UnaryMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(MultipleMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(StoragePhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(GlobalPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(IGinXPhysicalTask task) {
    collectTaskInfo(task);
  }

  private void collectTaskInfo(PhysicalTask task) {
    TaskType type = task.getType();
    StringBuilder builder = new StringBuilder();
    if (depth != 0) {
      for (int i = 0; i < depth; i++) {
        builder.append("  ");
      }
      builder.append("+--");
    }
    builder.append(type);

    maxLen = Math.max(maxLen, builder.length());

    Object[] values = new Object[5];
    values[0] = builder.toString();
    values[1] = (task.getSpan() + "ms").getBytes();
    values[2] = task.getType().toString().getBytes();
    values[3] = task.getInfo().getBytes();
    values[4] = task.getAffectedRows();
    cache.add(values);
  }
}
