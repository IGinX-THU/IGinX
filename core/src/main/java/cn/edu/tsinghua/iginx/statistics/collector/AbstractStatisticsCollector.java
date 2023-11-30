package cn.edu.tsinghua.iginx.statistics.collector;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.processor.Processor;
import cn.edu.tsinghua.iginx.statistics.Statistics;
import cn.edu.tsinghua.iginx.thrift.Status;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

public abstract class AbstractStatisticsCollector {

  protected static final String BEGIN = "Begin-";

  protected static final String END = "End-";

  private final LinkedBlockingQueue<Statistics> statisticsQueue = new LinkedBlockingQueue<>();

  protected Function<RequestContext, Status> before =
      requestContext -> {
        requestContext.setExtraParam(BEGIN + getCollectorType(), System.currentTimeMillis());
        return null;
      };

  protected Function<RequestContext, Status> after =
      requestContext -> {
        long endTime = System.currentTimeMillis();
        requestContext.setExtraParam(END + getCollectorType(), endTime);
        long startTime = (long) requestContext.getExtraParam(BEGIN + getCollectorType());
        statisticsQueue.add(
            new Statistics(requestContext.getId(), startTime, endTime, requestContext));
        return null;
      };

  public AbstractStatisticsCollector() {
    Executors.newSingleThreadExecutor()
        .submit(
            () -> {
              while (true) {
                Statistics statistics = statisticsQueue.take();
                processStatistics(statistics);
              }
            });
  }

  protected abstract CollectorType getCollectorType();

  protected abstract void processStatistics(Statistics statistics);

  public abstract void broadcastStatistics();

  public Processor getPreProcessor() {
    return before::apply;
  }

  public Processor getPostProcessor() {
    return after::apply;
  }
}
