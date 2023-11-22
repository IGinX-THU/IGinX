package cn.edu.tsinghua.iginx.statistics.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class OperatorStatisticsCollector {

  private static class StatsCollectorHolder {
    private static final OperatorStatisticsCollector instance = new OperatorStatisticsCollector();
  }

  public static OperatorStatisticsCollector getInstance() {
    return StatsCollectorHolder.instance;
  }

  private final Map<OperatorType, OperatorStatistics> statsMap = new HashMap<>();

  private final LinkedBlockingQueue<OperatorStatistics> statsQueue = new LinkedBlockingQueue<>();

  private OperatorStatisticsCollector() {
    Executors.newSingleThreadExecutor()
        .submit(
            () -> {
              while (true) {
                OperatorStatistics stats = statsQueue.take();
                processStats(stats);
              }
            });
  }

  private void processStats(OperatorStatistics stats) {
    OperatorType type = stats.getType();
    if (statsMap.containsKey(type)) {
      statsMap.get(type).addStats(stats);
    } else {
      statsMap.put(type, stats);
    }
  }

  public void addStats(OperatorStatistics stats) {
    statsQueue.add(stats);
  }

  public Map<OperatorType, OperatorStatistics> getStatsMap() {
    return statsMap;
  }
}
