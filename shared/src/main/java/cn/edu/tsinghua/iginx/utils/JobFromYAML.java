package cn.edu.tsinghua.iginx.utils;

import java.util.List;

public class JobFromYAML {

  private List<TaskFromYAML> taskList;
  private String exportFile;
  private String exportType;
  private String schedule;

  public JobFromYAML() {}

  public List<TaskFromYAML> getTaskList() {
    return taskList;
  }

  public void setTaskList(List<TaskFromYAML> taskList) {
    this.taskList = taskList;
  }

  public String getExportFile() {
    return exportFile;
  }

  public void setExportFile(String exportFile) {
    this.exportFile = exportFile;
  }

  public String getExportType() {
    return exportType;
  }

  public void setExportType(String exportType) {
    this.exportType = exportType;
  }

  public String getSchedule() {
    return schedule;
  }

  public void setSchedule(String schedule) {
    this.schedule = schedule;
  }
}
