package cn.edu.tsinghua.iginx.engine.physical.task;

public enum TaskType {
  Storage,
  Global,
  Memory,

  MultipleMemory,
  BinaryMemory,
  UnaryMemory;

  public static boolean isMemoryTask(TaskType type) {
    return type == MultipleMemory || type == BinaryMemory || type == UnaryMemory;
  }
}
