package cn.edu.tsinghua.iginx.statistics.collector;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.statistics.Statistics;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteInfoCollector extends AbstractStatisticsCollector {

  private static final Logger logger = LoggerFactory.getLogger(WriteInfoCollector.class);
  private static final CollectorType collectorType = CollectorType.WriteInfo;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private long count = 0;
  private long span = 0;
  private long pointsCount = 0;

  @Override
  protected CollectorType getCollectorType() {
    return collectorType;
  }

  @Override
  protected void processStatistics(Statistics statistics) {
    RequestContext ctx = statistics.getContext();
    if (!ctx.getSqlType().equals(SqlType.Insert)) {
      return;
    }

    lock.writeLock().lock();
    count += 1;
    span += statistics.getEndTime() - statistics.getStartTime();
    InsertStatement insertStatement = (InsertStatement) ctx.getStatement();
    pointsCount += (long) insertStatement.getPaths().size() * insertStatement.getKeys().size();
    lock.writeLock().unlock();
  }

  @Override
  public void broadcastStatistics() {
    lock.readLock().lock();
    logger.info(String.format("%s Statistics Info: ", collectorType));
    logger.info("\tcount: " + count + ", span: " + span + "ms, points: " + pointsCount);
    if (count != 0) {
      logger.info("\taverage-span: " + (1.0 * span) / count + "ms");
      logger.info("\taverage-points: " + (1.0 * pointsCount) / count + "ms");
    }
    lock.readLock().unlock();
  }
}
