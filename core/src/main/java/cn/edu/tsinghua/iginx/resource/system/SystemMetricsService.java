package cn.edu.tsinghua.iginx.resource.system;

public interface SystemMetricsService {

  void start();

  void stop();

  double getRecentCpuUsage();

  double getRecentMemoryUsage();
}
