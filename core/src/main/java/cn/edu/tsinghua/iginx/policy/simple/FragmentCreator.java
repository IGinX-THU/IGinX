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
package cn.edu.tsinghua.iginx.policy.simple;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FragmentCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentCreator.class);

  private static Timer timer = new Timer();

  private final IMetaManager iMetaManager;
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private final SimplePolicy policy;

  public FragmentCreator(SimplePolicy policy, IMetaManager iMetaManager) {
    this.policy = policy;
    this.iMetaManager = iMetaManager;
    init(config.getReAllocatePeriod());
  }

  public boolean waitforUpdate(int version) {
    int retry = config.getRetryCount();
    while (retry > 0) {
      Map<Integer, Integer> timeseriesVersionMap = iMetaManager.getColumnsVersionMap();
      Set<Integer> idSet =
          iMetaManager.getIginxList().stream()
              .map(IginxMeta::getId)
              .map(Long::intValue)
              .collect(Collectors.toSet());
      if (version
          <= timeseriesVersionMap.entrySet().stream()
              .filter(e -> idSet.contains(e.getKey()))
              .map(Map.Entry::getValue)
              .min(Integer::compareTo)
              .orElse(Integer.MAX_VALUE)) {
        return true;
      }
      LOGGER.info(
          "retry, remain: {}, version:{}, minversion: {}",
          retry,
          version,
          timeseriesVersionMap.values().stream().min(Integer::compareTo).orElse(Integer.MAX_VALUE));
      try {
        Thread.sleep(config.getRetryWait());
      } catch (InterruptedException e) {
        LOGGER.error("unexpected error: ", e);
      }
      retry--;
    }
    return false;
  }

  public void createFragment() throws Exception {
    LOGGER.info("start CreateFragment");
    if (iMetaManager.election()) {
      int version = iMetaManager.updateVersion();
      if (version > 0) {
        if (!waitforUpdate(version)) {
          LOGGER.error("update failed");
          return;
        }
        if (!policy.checkSuccess(iMetaManager.getColumnsData())) {
          policy.setNeedReAllocate(true);
          LOGGER.info("set ReAllocate true");
        }
      }
    }
    LOGGER.info("end CreateFragment");
  }

  public void init(int length) {
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            try {
              createFragment();
            } catch (Exception e) {
              LOGGER.error("unexpected error: ", e);
            }
          }
        },
        length,
        length);
  }
}
