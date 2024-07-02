package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class PythonTask extends Task {

  private String pyTaskName = "";

  public PythonTask(TaskInfo info) {
    super(info);
    if (info.isSetPyTaskName()) {
      pyTaskName = info.getPyTaskName().trim();
    } else {
      throw new IllegalArgumentException("Python task must have class name.");
    }
  }

  public PythonTask(TaskFromYAML info) {
    super(info);
    if (info.getPyTaskName() != null) {
      pyTaskName = info.getPyTaskName().trim();
    } else {
      throw new IllegalArgumentException("Python task must have class name.");
    }
  }

  // for test
  public PythonTask(
      TaskType taskType, DataFlowType dataFlowType, long timeLimit, String pyTaskName) {
    super(taskType, dataFlowType, timeLimit);
    this.pyTaskName = pyTaskName.trim();
  }

  public String getPyTaskName() {
    return pyTaskName;
  }
}
