/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.processor.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsCollector implements IStatisticsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCollector.class);

  private final AtomicBoolean broadcast = new AtomicBoolean(false);

  private final ExecutorService broadcastThreadPool = Executors.newSingleThreadExecutor();

  private final ParseStatisticsCollector parseStatisticsCollector = new ParseStatisticsCollector();
  private final LogicalStatisticsCollector logicalStatisticsCollector =
      new LogicalStatisticsCollector();
  private final PhysicalStatisticsCollector physicalStatisticsCollector =
      new PhysicalStatisticsCollector();
  private final ExecuteStatisticsCollector executeStatisticsCollector =
      new ExecuteStatisticsCollector();

  @Override
  public PreLogicalProcessor getPreLogicalProcessor() {
    return logicalStatisticsCollector.getPreLogicalProcessor();
  }

  @Override
  public PostLogicalProcessor getPostLogicalProcessor() {
    return logicalStatisticsCollector.getPostLogicalProcessor();
  }

  @Override
  public PreParseProcessor getPreParseProcessor() {
    return parseStatisticsCollector.getPreParseProcessor();
  }

  @Override
  public PostParseProcessor getPostParseProcessor() {
    return parseStatisticsCollector.getPostParseProcessor();
  }

  @Override
  public PrePhysicalProcessor getPrePhysicalProcessor() {
    return physicalStatisticsCollector.getPrePhysicalProcessor();
  }

  @Override
  public PostPhysicalProcessor getPostPhysicalProcessor() {
    return physicalStatisticsCollector.getPostPhysicalProcessor();
  }

  @Override
  public PreExecuteProcessor getPreExecuteProcessor() {
    return executeStatisticsCollector.getPreExecuteProcessor();
  }

  @Override
  public PostExecuteProcessor getPostExecuteProcessor() {
    return executeStatisticsCollector.getPostExecuteProcessor();
  }

  @Override
  public void startBroadcasting() {
    broadcast.set(true);
    // 启动一个新线程，定期播报统计信息
    broadcastThreadPool.execute(
        () -> {
          try {
            while (broadcast.get()) {
              parseStatisticsCollector.broadcastStatistics();
              logicalStatisticsCollector.broadcastStatistics();
              physicalStatisticsCollector.broadcastStatistics();
              executeStatisticsCollector.broadcastStatistics();
              Thread.sleep(
                  ConfigDescriptor.getInstance()
                      .getConfig()
                      .getStatisticsLogInterval()); // 每隔 10 秒播报一次统计信息
            }
          } catch (InterruptedException e) {
            LOGGER.error("encounter error when broadcasting statistics: ", e);
          }
        });
  }

  @Override
  public void endBroadcasting() {
    broadcast.set(false);
  }
}
