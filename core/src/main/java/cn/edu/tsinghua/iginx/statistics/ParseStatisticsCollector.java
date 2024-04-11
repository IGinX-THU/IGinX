package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostParseProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreParseProcessor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseStatisticsCollector extends AbstractStageStatisticsCollector
    implements IParseStatisticsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParseStatisticsCollector.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private long count = 0;
  private long span = 0;

  @Override
  protected String getStageName() {
    return "ParseSQLStage";
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
    LOGGER.info("Parse Stage Statistics Info: ");
    LOGGER.info("\tcount: {}, span: {}μs", count, span);
    if (count != 0) {
      LOGGER.info("\taverage-span: {}μs", (1.0 * span) / count);
    }
    lock.readLock().unlock();
  }

  @Override
  public PreParseProcessor getPreParseProcessor() {
    return before::apply;
  }

  @Override
  public PostParseProcessor getPostParseProcessor() {
    return after::apply;
  }
}
