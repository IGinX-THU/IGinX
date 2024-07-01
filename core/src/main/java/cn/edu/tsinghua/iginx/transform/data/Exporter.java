package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.transform.utils.Mutex;

public interface Exporter {

  Mutex getMutex();

  // reset state for next scheduled run
  void reset();
}
