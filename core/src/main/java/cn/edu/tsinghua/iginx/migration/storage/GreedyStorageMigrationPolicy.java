package cn.edu.tsinghua.iginx.migration.storage;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class GreedyStorageMigrationPolicy extends StorageMigrationPolicy {

    private static final Logger logger = LoggerFactory.getLogger(GreedyStorageMigrationPolicy.class);

    public GreedyStorageMigrationPolicy(IMetaManager metaManager) {
        super(metaManager);
    }

    static class StoragePriority implements Comparable<StoragePriority> {

        long storageId;

        int weight;

        StoragePriority(long storageId, int weight) {
            this.storageId = storageId;
            this.weight = weight;
        }

        @Override
        public int compareTo(StoragePriority o) {
            return weight - o.weight;
        }
    }

    @Override
    public StorageMigrationPlan generateMigrationPlans(long sourceStorageId, boolean migrationData) {
        logger.info("[FaultTolerance][MigrationPolicy][iginx={}, id={}] generate migration plan", metaManager.getIginxId(), sourceStorageId);
        for (StorageUnitMeta meta: metaManager.getStorageUnits()) {
            logger.info("[FaultTolerance][MigrationPolicy][iginx={}, id={}] current storage unit info: {}", metaManager.getIginxId(), sourceStorageId, meta.toString());
        }
        // discard、dummy 的分片不需要迁移，只迁移很正常状态的分片
        Map<Long, List<StorageUnitMeta>> storageUnitsMap = metaManager.getStorageUnits().stream().filter(e -> !e.isDummy()).filter(e -> e.getState() != StorageUnitState.DISCARD).collect(Collectors.groupingBy(StorageUnitMeta::getStorageEngineId));

        List<StorageUnitMeta> storageUnits = storageUnitsMap.get(sourceStorageId);
        for (StorageUnitMeta meta: storageUnits) {
            logger.info("[FaultTolerance][MigrationPolicy][iginx={}, id={}] du={} need to migration.", metaManager.getIginxId(), sourceStorageId, meta.getId());
        }

        List<StoragePriority> priorities = new ArrayList<>();
        for (long storageId: storageUnitsMap.keySet()) {
            if (storageId == sourceStorageId) {
                continue;
            }
            priorities.add(new StoragePriority(storageId, storageUnitsMap.get(storageId).size()));
        }
        Map<String, Long> migrationMap = new HashMap<>();

        for (StorageUnitMeta storageUnit: storageUnits) {
            priorities.sort(StoragePriority::compareTo);
            long avoid = sourceStorageId;
            if (!storageUnit.isMaster()) {
                StorageUnitMeta masterUnit = metaManager.getStorageUnit(storageUnit.getMasterId());
                avoid = masterUnit.getStorageEngineId();
            } else {
                for (StorageUnitMeta unit: metaManager.getStorageUnits()) {
                    if (unit.getState() == StorageUnitState.DISCARD) {
                        continue;
                    }
                    if (!unit.isMaster() && Objects.equals(unit.getMasterId(), storageUnit.getId())) {
                        avoid = unit.getStorageEngineId();
                        break;
                    }
                }
            }
            long migrationTo = 0;
            for (StoragePriority priority: priorities) {
                if (priority.storageId != avoid) {
                    priority.weight++;
                    migrationTo = priority.storageId;
                    break;
                }
            }
            logger.info("[FaultTolerance][MigrationPolicy][iginx={}, id={}] du={} avoid migration to {}, decide migration to {}", metaManager.getIginxId(), sourceStorageId, storageUnit.getId(), avoid, migrationTo);
            migrationMap.put(storageUnit.getId(), migrationTo);
        }
        return new StorageMigrationPlan(sourceStorageId, migrationData, migrationMap, metaManager.getIginxId());
    }
}
