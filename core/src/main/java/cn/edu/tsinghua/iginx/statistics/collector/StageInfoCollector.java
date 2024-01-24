package cn.edu.tsinghua.iginx.statistics.collector;

import cn.edu.tsinghua.iginx.statistics.Statistics;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StageInfoCollector extends AbstractStatisticsCollector {

  private static final Logger logger = LoggerFactory.getLogger(StageInfoCollector.class);
  private final CollectorType collectorType;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private long count = 0;
  private long span = 0;

  public StageInfoCollector(CollectorType collectorType) {
    this.collectorType = collectorType;
  }

  @Override
  protected CollectorType getCollectorType() {
    return collectorType;
  }

  @Override
  protected void processStatistics(Statistics statistics) {
    lock.writeLock().lock();
    count += 1;
    span += statistics.getEndTime() - statistics.getStartTime();
    lock.writeLock().unlock();
  }

  @Override
  public void broadcastStatistics() {
    lock.readLock().lock();
    logger.info(String.format("%s Statistics Info: ", collectorType));
    logger.info("\tcount: " + count + ", span: " + span + "ms");
    if (count != 0) {
      logger.info("\taverage-span: " + (1.0 * span) / count + "ms");
    }
    lock.readLock().unlock();
  }
}
