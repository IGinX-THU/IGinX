package cn.edu.tsinghua.iginx.transform.api;

import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import org.quartz.SchedulerException;

public interface Runner {

  void start() throws TransformException, SchedulerException;

  void run() throws TransformException;

  void close() throws TransformException;

  boolean scheduled();
}
