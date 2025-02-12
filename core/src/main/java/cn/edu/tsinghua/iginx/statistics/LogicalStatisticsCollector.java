/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostLogicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreLogicalProcessor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalStatisticsCollector extends AbstractStageStatisticsCollector
    implements ILogicalStatisticsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalStatisticsCollector.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private long count = 0;
  private long span = 0;

  @Override
  protected String getStageName() {
    return "LogicalStage";
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
    LOGGER.info("Logical Stage Statistics Info: ");
    LOGGER.info("\tcount: {}, span: {}μs", count, span);
    if (count != 0) {
      LOGGER.info("\taverage-span: {}}μs", (1.0 * span) / count);
    }
    lock.readLock().unlock();
  }

  @Override
  public PreLogicalProcessor getPreLogicalProcessor() {
    return before::apply;
  }

  @Override
  public PostLogicalProcessor getPostLogicalProcessor() {
    return after::apply;
  }
}
