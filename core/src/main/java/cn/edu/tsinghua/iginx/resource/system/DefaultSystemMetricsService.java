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
package cn.edu.tsinghua.iginx.resource.system;

import cn.hutool.system.oshi.OshiUtil;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import oshi.hardware.GlobalMemory;

public class DefaultSystemMetricsService implements SystemMetricsService {

  private static final int SAMPLE_SIZE = 60;

  private static final int UPDATE_PER_SAMPLE = 10; // UPDATE_PER_SAMPLE 必须能整除 SAMPLE_SIZE

  private static final long STATISTICS_INTERVAL = 1000L;

  private final double[] cpuUsages;

  private final double[] memoryUsage;

  private final AtomicDouble recentCpuUsage;

  private final AtomicDouble recentMemoryUsage;

  private final ExecutorService exec;

  private int index = 0;

  public DefaultSystemMetricsService() {
    recentCpuUsage = new AtomicDouble(0.0);
    recentMemoryUsage = new AtomicDouble(0.0);
    cpuUsages = new double[SAMPLE_SIZE];
    memoryUsage = new double[SAMPLE_SIZE];
    exec = Executors.newSingleThreadExecutor();
  }

  @Override
  public void start() {
    exec.execute(
        () -> {
          cpuUsages[index] = (100.0 - OshiUtil.getCpuInfo(STATISTICS_INTERVAL).getFree()) / 100.0;
          GlobalMemory memory = OshiUtil.getMemory();
          memoryUsage[index] =
              (memory.getTotal() - memory.getAvailable()) * 1.0 / memory.getTotal();
          index++;
          if (index % UPDATE_PER_SAMPLE == 0) {
            recentCpuUsage.set(avg(cpuUsages));
            recentMemoryUsage.set(avg(memoryUsage));
          }
          index %= SAMPLE_SIZE;
        });
  }

  @Override
  public void stop() {
    exec.shutdown();
  }

  @Override
  public double getRecentCpuUsage() {
    return recentCpuUsage.get();
  }

  @Override
  public double getRecentMemoryUsage() {
    return recentMemoryUsage.get();
  }

  private static double avg(double[] arr) {
    if (arr == null || arr.length == 0) {
      return 0.0;
    }
    double sum = 0.0;
    for (double value : arr) {
      sum += value;
    }
    return sum / arr.length;
  }
}
