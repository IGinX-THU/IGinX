package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.resource.system.DefaultSystemMetricsService;
import cn.edu.tsinghua.iginx.resource.system.SystemMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManager.class);

  private final SystemMetricsService systemMetrics;

  private final double heapMemoryThreshold;

  private final double systemMemoryThreshold;

  private final double systemCpuThreshold;

  private ResourceManager() {
    Config config = ConfigDescriptor.getInstance().getConfig();
    switch (config.getSystemResourceMetrics()) {
      case "default":
        systemMetrics = new DefaultSystemMetricsService();
        break;
      default:
        LOGGER.info("use DefaultSystemMetrics as default");
        systemMetrics = new DefaultSystemMetricsService();
        break;
    }
    heapMemoryThreshold = config.getHeapMemoryThreshold();
    systemMemoryThreshold = config.getSystemMemoryThreshold();
    systemCpuThreshold = config.getSystemCpuThreshold();
  }

  public boolean reject(RequestContext ctx) {
    return heapMemoryOverwhelmed()
        || systemMetrics.getRecentCpuUsage() > systemCpuThreshold
        || systemMetrics.getRecentMemoryUsage() > systemMemoryThreshold;
  }

  private boolean heapMemoryOverwhelmed() {
    return Runtime.getRuntime().totalMemory() * heapMemoryThreshold
        > Runtime.getRuntime().maxMemory();
  }

  public static ResourceManager getInstance() {
    return ResourceManagerHolder.INSTANCE;
  }

  private static class ResourceManagerHolder {

    private static final ResourceManager INSTANCE = new ResourceManager();
  }
}
