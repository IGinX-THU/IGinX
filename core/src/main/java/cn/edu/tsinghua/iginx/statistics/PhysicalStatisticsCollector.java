package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostPhysicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PrePhysicalProcessor;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhysicalStatisticsCollector extends AbstractStageStatisticsCollector
    implements IPhysicalStatisticsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalStatisticsCollector.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<StatementType, Pair<Long, Long>> detailInfos = new HashMap<>();
  private long count = 0;
  private long span = 0;

  @Override
  protected String getStageName() {
    return "PhysicalStage";
  }

  @Override
  protected void processStatistics(Statistics statistics) {
    lock.writeLock().lock();
    count += 1;
    span += statistics.getEndTime() - statistics.getStartTime();
    Pair<Long, Long> detailInfo =
        detailInfos.computeIfAbsent(
            statistics.getContext().getStatement().getType(), e -> new Pair<>(0L, 0L));
    detailInfo.k += 1;
    detailInfo.v += statistics.getEndTime() - statistics.getStartTime();
    lock.writeLock().unlock();
  }

  @Override
  public void broadcastStatistics() {
    lock.readLock().lock();
    LOGGER.info("Physical Stage Statistics Info: ");
    LOGGER.info("\tcount: {}, span: {}μs", count, span);
    if (count != 0) {
      LOGGER.info("\taverage-span: {}μs", (1.0 * span) / count);
    }
    for (Map.Entry<StatementType, Pair<Long, Long>> entry : detailInfos.entrySet()) {
      LOGGER.info(
          "\t\tFor Request: "
              + entry.getKey()
              + ", count: "
              + entry.getValue().k
              + ", span: "
              + entry.getValue().v
              + "μs");
    }
    lock.readLock().unlock();
  }

  @Override
  public PrePhysicalProcessor getPrePhysicalProcessor() {
    return before::apply;
  }

  @Override
  public PostPhysicalProcessor getPostPhysicalProcessor() {
    return after::apply;
  }
}
