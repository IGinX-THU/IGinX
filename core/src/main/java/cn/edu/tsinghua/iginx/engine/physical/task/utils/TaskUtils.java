package cn.edu.tsinghua.iginx.engine.physical.task.utils;

import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.CompletedFoldedPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.FoldedMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import java.util.List;

public class TaskUtils {

  public static void getStorageTasks(List<StoragePhysicalTask> tasks, PhysicalTask root) {
    if (root == null) {
      return;
    }
    switch (root.getType()) {
      case Storage:
        tasks.add((StoragePhysicalTask) root);
        break;
      case BinaryMemory:
        BinaryMemoryPhysicalTask binaryMemoryPhysicalTask = (BinaryMemoryPhysicalTask) root;
        getStorageTasks(tasks, binaryMemoryPhysicalTask.getParentTaskA());
        getStorageTasks(tasks, binaryMemoryPhysicalTask.getParentTaskB());
        break;
      case UnaryMemory:
        UnaryMemoryPhysicalTask unaryMemoryPhysicalTask = (UnaryMemoryPhysicalTask) root;
        getStorageTasks(tasks, unaryMemoryPhysicalTask.getParentTask());
        break;
      case MultipleMemory:
        MultipleMemoryPhysicalTask multipleMemoryPhysicalTask = (MultipleMemoryPhysicalTask) root;
        for (PhysicalTask parentTask : multipleMemoryPhysicalTask.getParentTasks()) {
          getStorageTasks(tasks, parentTask);
        }
        break;
      case Folded:
        FoldedMemoryPhysicalTask foldedMemoryPhysicalTask = (FoldedMemoryPhysicalTask) root;
        for (PhysicalTask parentTask : foldedMemoryPhysicalTask.getParentTasks()) {
          getStorageTasks(tasks, parentTask);
        }
        break;
      case CompletedFolded:
        CompletedFoldedPhysicalTask completedFoldedPhysicalTask =
            (CompletedFoldedPhysicalTask) root;
        getStorageTasks(tasks, completedFoldedPhysicalTask.getParentTask());
        break;
      default:
        throw new RuntimeException("unknown task type: " + root.getType());
    }
  }
}
