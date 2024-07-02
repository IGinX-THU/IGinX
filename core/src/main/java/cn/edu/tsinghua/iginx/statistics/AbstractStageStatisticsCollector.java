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

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.thrift.Status;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStageStatisticsCollector {
  @SuppressWarnings("unused")
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractStageStatisticsCollector.class);

  protected static final String BEGIN = "begin";

  protected static final String END = "end";

  private final LinkedBlockingQueue<Statistics> statisticsQueue = new LinkedBlockingQueue<>();

  protected Function<RequestContext, Status> before =
      requestContext -> {
        requestContext.setExtraParam(BEGIN + getStageName(), System.currentTimeMillis());
        return null;
      };

  protected Function<RequestContext, Status> after =
      requestContext -> {
        long endTime = System.currentTimeMillis();
        requestContext.setExtraParam(END + getStageName(), endTime);
        long startTime = (long) requestContext.getExtraParam(BEGIN + getStageName());
        statisticsQueue.add(
            new Statistics(requestContext.getId(), startTime, endTime, requestContext));
        return null;
      };

  public AbstractStageStatisticsCollector() {
    Executors.newSingleThreadExecutor()
        .submit(
            () -> {
              while (true) {
                Statistics statistics = statisticsQueue.take();
                processStatistics(statistics);
              }
            });
  }

  protected abstract String getStageName();

  protected abstract void processStatistics(Statistics statistics);

  public abstract void broadcastStatistics();
}
