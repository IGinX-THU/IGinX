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
package cn.edu.tsinghua.iginx.engine.physical.storage.reconnect;

import org.quartz.JobDetail;

public class ReconnectState {

  private static final long INITIAL_RECONNECT_INTERVAL = 2; // 初始重连间隔（秒）

  private static final long MAX_RECONNECT_INTERVAL = 128; // 最大重连间隔（秒）

  private static final int BACKOFF_MULTIPLIER = 2; // 重连间隔增长倍数

  private final JobDetail jobDetail;

  private long nextInterval;

  ReconnectState(JobDetail jobDetail) {
    this.jobDetail = jobDetail;
    this.nextInterval = INITIAL_RECONNECT_INTERVAL;
  }

  public long calculateNextInterval() {
    if (nextInterval < MAX_RECONNECT_INTERVAL) {
      nextInterval = Math.min(nextInterval * BACKOFF_MULTIPLIER, MAX_RECONNECT_INTERVAL);
    }
    return nextInterval;
  }

  public JobDetail getJobDetail() {
    return jobDetail;
  }
}
