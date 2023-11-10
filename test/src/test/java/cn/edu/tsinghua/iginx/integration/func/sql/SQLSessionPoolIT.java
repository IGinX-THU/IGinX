package cn.edu.tsinghua.iginx.integration.func.sql;

import java.io.IOException;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import org.junit.*;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;

public class SQLSessionPoolIT extends SQLSessionIT {
  public SQLSessionPoolIT() throws IOException {
    super();
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (!SUPPORT_KEY.get(conf.getStorageType()) && this.isScaling) {
      needCompareResult = false;
      executor.setNeedCompareResult(needCompareResult);
    }
    isForSessionPool = true;
    isForSession = false;
    MaxMultiThreadTaskNum = 10;
  }
}
