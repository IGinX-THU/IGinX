package cn.edu.tsinghua.iginx.engine.shared.stats;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class StatsCollector {

  private static class StatsCollectorHolder {
    private static final StatsCollector instance = new StatsCollector();
  }

  public static StatsCollector getInstance() {
    return StatsCollectorHolder.instance;
  }

  private final Map<OperatorType, OperatorStats> statsMap = new HashMap<>();

  private final LinkedBlockingQueue<OperatorStats> statsQueue = new LinkedBlockingQueue<>();

  private StatsCollector() {
    Executors.newSingleThreadExecutor()
        .submit(
            () -> {
              while (true) {
                OperatorStats stats = statsQueue.take();
                processStats(stats);
              }
            });
  }

  private void processStats(OperatorStats stats) {
    OperatorType type = stats.getType();
    if (statsMap.containsKey(type)) {
      statsMap.get(type).addStats(stats);
    } else {
      statsMap.put(type, stats);
    }
  }

  public void addStats(OperatorStats stats) {
    statsQueue.add(stats);
  }

  public Map<OperatorType, OperatorStats> getStatsMap() {
    return statsMap;
  }
}
