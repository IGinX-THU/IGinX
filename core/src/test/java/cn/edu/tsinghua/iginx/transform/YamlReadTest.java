package cn.edu.tsinghua.iginx.transform;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
import java.io.FileNotFoundException;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YamlReadTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(YamlReadTest.class);

  final String filePath = "../example/src/main/resources/TransformJobExample.yaml";

  private static final String[] taskTypeArr = {"iginx", "python", "python", "python"};

  private static final String[] dataFlowTypeArr = {"null", "stream", "batch", "stream"};

  private static final long[] timeoutArr = {10000000, 10000000, 10000000, 10000000};

  private static final String[] pyTaskNameArr = {
    "null", "AddOneTransformer", "SumTransformer", "RowSumTransformer"
  };

  private static final String[] sqlListArr = {
    "select value1, value2, value3, value4 from transform;", "null", "null", "null"
  };

  @Test
  public void test() {
    try {
      YAMLReader yamlReader = new YAMLReader(filePath);
      JobFromYAML jobFromYAML = yamlReader.getJobFromYAML();
      List<TaskFromYAML> taskList = jobFromYAML.getTaskList();

      assertEquals("file", jobFromYAML.getExportType());
      assertEquals(
          "/Users/cauchy-ny/Downloads/export_file_sum_sql.txt", jobFromYAML.getExportFile());

      for (int i = 0; i < taskList.size(); i++) {
        assertEquals(taskTypeArr[i], taskList.get(i).getTaskType());
        assertEquals(
            dataFlowTypeArr[i],
            taskList.get(i).getDataFlowType() == null ? "null" : taskList.get(i).getDataFlowType());
        assertEquals(timeoutArr[i], taskList.get(i).getTimeout());
        assertEquals(
            pyTaskNameArr[i],
            taskList.get(i).getPyTaskName() == null ? "null" : taskList.get(i).getPyTaskName());
        assertEquals(
            sqlListArr[i],
            taskList.get(i).getSqlList() == null
                ? "null"
                : String.join(" ", taskList.get(i).getSqlList()));
      }
    } catch (FileNotFoundException e) {
      LOGGER.error("Fail to close the file, path={}", filePath);
    }
  }
}
