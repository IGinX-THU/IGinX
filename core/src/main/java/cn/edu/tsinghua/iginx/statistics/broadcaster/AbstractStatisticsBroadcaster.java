package cn.edu.tsinghua.iginx.statistics.broadcaster;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStatisticsBroadcaster implements Broadcaster {

  private static final Logger logger = LoggerFactory.getLogger(AbstractStatisticsBroadcaster.class);

  private final AtomicBoolean broadcast = new AtomicBoolean(false);

  private final ExecutorService broadcastThreadPool = Executors.newSingleThreadExecutor();

  /* scheduled task logic implements here */
  protected abstract void broadcasting();

  @Override
  public void startBroadcasting() {
    broadcast.set(true);
    // starting a thread to do the scheduled tasks
    broadcastThreadPool.execute(
        () -> {
          try {
            while (broadcast.get()) {
              broadcasting();
              Thread.sleep(ConfigDescriptor.getInstance().getConfig().getStatisticsLogInterval());
            }
          } catch (InterruptedException e) {
            logger.error("encounter error when broadcasting statistics: ", e);
          }
        });
  }

  @Override
  public void endBroadcasting() {
    broadcast.set(false);
  }
}
