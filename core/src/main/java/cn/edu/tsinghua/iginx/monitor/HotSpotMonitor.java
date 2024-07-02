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
package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HotSpotMonitor implements IMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(HotSpotMonitor.class);

  private final boolean isEnableMonitor =
      ConfigDescriptor.getInstance().getConfig().isEnableMonitor();
  private final Map<FragmentMeta, Long> writeHotspotMap =
      new ConcurrentHashMap<>(); // 数据分区->写入总请求时间
  private final Map<FragmentMeta, Long> readHotspotMap = new ConcurrentHashMap<>(); // 数据分区->查询总请求时间
  private static final HotSpotMonitor instance = new HotSpotMonitor();

  public static HotSpotMonitor getInstance() {
    return instance;
  }

  public Map<FragmentMeta, Long> getWriteHotspotMap() {
    return writeHotspotMap;
  }

  public Map<FragmentMeta, Long> getReadHotspotMap() {
    return readHotspotMap;
  }

  public void recordAfter(long taskId, FragmentMeta fragmentMeta, OperatorType operatorType) {
    if (isEnableMonitor) {
      long duration = (System.nanoTime() - taskId) / 1000000;
      if (operatorType == OperatorType.Project) {
        long prevDuration = readHotspotMap.getOrDefault(fragmentMeta, 0L);
        readHotspotMap.put(fragmentMeta, prevDuration + duration);
      } else if (operatorType == OperatorType.Insert) {
        long prevDuration = writeHotspotMap.getOrDefault(fragmentMeta, 0L);
        writeHotspotMap.put(fragmentMeta, prevDuration + duration);
      }
    }
  }

  @Override
  public void clear() {
    writeHotspotMap.clear();
    readHotspotMap.clear();
  }
}
