package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.api.Checker;
import cn.edu.tsinghua.iginx.transform.pojo.IginXTask;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobValidationChecker implements Checker {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobValidationChecker.class);

  private static JobValidationChecker instance;

  private JobValidationChecker() {}

  public static JobValidationChecker getInstance() {
    if (instance == null) {
      synchronized (JobValidationChecker.class) {
        if (instance == null) {
          instance = new JobValidationChecker();
        }
      }
    }
    return instance;
  }

  @Override
  public boolean check(Job job) {
    List<Task> taskList = job.getTaskList();
    if (taskList == null || taskList.isEmpty()) {
      LOGGER.error("Committed job task list is empty.");
      return false;
    }

    Task firstTask = taskList.get(0);
    if (!firstTask.getTaskType().equals(TaskType.IginX)) {
      LOGGER.error("The first task must be IginX task.");
      return false;
    }

    if (!firstTask.getDataFlowType().equals(DataFlowType.Stream)) {
      LOGGER.error("The IginX task must be stream.");
      return false;
    }

    IginXTask iginXTask = (IginXTask) firstTask;
    List<String> sqlList = iginXTask.getSqlList();
    if (sqlList == null || sqlList.isEmpty()) {
      LOGGER.error("The first task should has at least one statement.");
      return false;
    }

    String querySQL = sqlList.get(sqlList.size() - 1);
    if (!querySQL.toLowerCase().trim().startsWith("select")
        && !querySQL.toLowerCase().trim().startsWith("show")) {
      LOGGER.error("The first task's last statement must be select or showTS statement.");
      return false;
    }

    if (taskList.size() > 1) {
      for (int i = 1; i < taskList.size(); i++) {
        if (taskList.get(i).getTaskType().equals(TaskType.IginX)) {
          LOGGER.error("2-n tasks must be python tasks.");
          return false;
        }
      }
    }

    return true;
  }
}
