package cn.edu.tsinghua.iginx.statistics.broadcaster;

import cn.edu.tsinghua.iginx.engine.shared.processor.Processor;
import cn.edu.tsinghua.iginx.statistics.collector.AbstractStatisticsCollector;
import cn.edu.tsinghua.iginx.statistics.collector.CollectorType;
import cn.edu.tsinghua.iginx.statistics.collector.OperatorInfoCollector;
import cn.edu.tsinghua.iginx.statistics.collector.StageInfoCollector;
import cn.edu.tsinghua.iginx.statistics.collector.WriteInfoCollector;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatisticsBroadcaster extends AbstractStatisticsBroadcaster {

  private final Map<CollectorType, AbstractStatisticsCollector> collectorMap = new HashMap<>();

  private static class StatisticsCollectorHolder {
    private static final StatisticsBroadcaster instance = new StatisticsBroadcaster();
  }

  public static StatisticsBroadcaster getInstance() {
    return StatisticsCollectorHolder.instance;
  }

  private StatisticsBroadcaster() {
    List<CollectorType> collectorTypes =
        Arrays.asList(
            CollectorType.ParseStage,
            CollectorType.LogicalStage,
            CollectorType.PhysicalStage,
            CollectorType.ExecuteStage);
    for (CollectorType collectorType : collectorTypes) {
      collectorMap.put(collectorType, new StageInfoCollector(collectorType));
    }
    collectorMap.put(CollectorType.OperatorInfo, new OperatorInfoCollector());
    collectorMap.put(CollectorType.WriteInfo, new WriteInfoCollector());
  }

  @Override
  protected void broadcasting() {
    collectorMap.forEach((k, v) -> v.broadcastStatistics());
  }

  public Processor getPreProcessor(CollectorType type) {
    return collectorMap.get(type).getPreProcessor();
  }

  public Processor getPostProcessor(CollectorType type) {
    return collectorMap.get(type).getPostProcessor();
  }
}
