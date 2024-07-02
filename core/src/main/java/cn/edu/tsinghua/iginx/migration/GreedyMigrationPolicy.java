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
package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreedyMigrationPolicy extends MigrationPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(GreedyMigrationPolicy.class);

  public GreedyMigrationPolicy() {
    super(LOGGER);
  }

  @Override
  public void migrate(
      List<MigrationTask> migrationTasks,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap,
      Map<FragmentMeta, Long> fragmentReadLoadMap) {
    long startTime = System.currentTimeMillis();

    LOGGER.error("start to migrate and calculateNodeLoadMap");
    Map<Long, Long> nodeLoadMap =
        calculateNodeLoadMap(nodeFragmentMap, fragmentWriteLoadMap, fragmentReadLoadMap);
    LOGGER.error("start to createParallelQueueByPriority");
    List<Queue<MigrationTask>> migrationTaskQueueList =
        createParallelQueueByPriority(migrationTasks);

    executor = Executors.newCachedThreadPool();

    while (!isAllQueueEmpty(migrationTaskQueueList)) {
      LOGGER.error("start to executeOneRoundMigration");
      executeOneRoundMigration(migrationTaskQueueList, nodeLoadMap);
    }

    LOGGER.error(
        "complete all migration task with time consumption: {} ms",
        System.currentTimeMillis() - startTime);
  }

  @Override
  public void interrupt() {}

  private List<Queue<MigrationTask>> createParallelQueueByPriority(
      List<MigrationTask> migrationTasks) {
    Map<String, List<MigrationTask>> edgeMigrationTasksMap = new HashMap<>();
    for (MigrationTask migrationTask : migrationTasks) {
      String edgeId = migrationTask.getSourceStorageId() + "-" + migrationTask.getTargetStorageId();
      List<MigrationTask> edgeMigrationTasks =
          edgeMigrationTasksMap.computeIfAbsent(edgeId, k -> new ArrayList<>());
      edgeMigrationTasks.add(migrationTask);
    }
    List<Queue<MigrationTask>> results = new ArrayList<>();
    for (Entry<String, List<MigrationTask>> edgeMigrationTasksEntry :
        edgeMigrationTasksMap.entrySet()) {
      List<MigrationTask> edgeMigrationTasks = edgeMigrationTasksEntry.getValue();
      // 倒序排列
      edgeMigrationTasks.sort((o1, o2) -> (int) (o2.getPriorityScore() - o1.getPriorityScore()));
      Queue<MigrationTask> migrationTaskQueue = new LinkedBlockingQueue<>(edgeMigrationTasks);
      results.add(migrationTaskQueue);
    }
    sortQueueListByFirstItem(results);
    return results;
  }
}
