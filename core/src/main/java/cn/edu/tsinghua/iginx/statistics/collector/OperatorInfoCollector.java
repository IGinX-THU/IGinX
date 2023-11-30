package cn.edu.tsinghua.iginx.statistics.collector;

import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskStatsVisitor;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.statistics.Statistics;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorInfoCollector extends AbstractStatisticsCollector {

  private static final Logger logger = LoggerFactory.getLogger(OperatorInfoCollector.class);

  private static final CollectorType collectorType = CollectorType.OperatorInfo;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<OperatorType, Long> operatorCounterMap = new HashMap<>();

  private final Map<OperatorType, Long> operatorSpanMap = new HashMap<>();

  private final Map<OperatorType, Long> operatorRowsCountMap = new HashMap<>();

  @Override
  protected CollectorType getCollectorType() {
    return collectorType;
  }

  @Override
  protected void processStatistics(Statistics statistics) {
    lock.writeLock().lock();
    TaskVisitor visitor =
        new TaskStatsVisitor(operatorCounterMap, operatorSpanMap, operatorRowsCountMap);
    statistics.getContext().getPhysicalTree().accept(visitor);
    lock.writeLock().unlock();
  }

  @Override
  public void broadcastStatistics() {
    lock.readLock().lock();
    logger.info(String.format("%s Statistics Info: ", collectorType));

    operatorCounterMap.forEach(
        (type, count) -> {
          long span = operatorSpanMap.get(type);
          logger.info("\ttype: " + type + ", count: " + count + ", span: " + span + "ms");
          if (count != 0) {
            logger.info("\taverage-span: " + (1.0 * span) / count + "ms");
          }
        });

    lock.readLock().unlock();
  }
}
