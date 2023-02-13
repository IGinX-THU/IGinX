package cn.edu.tsinghua.iginx.migration.storage;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.migration.MigrationManager;
import cn.edu.tsinghua.iginx.migration.MigrationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class StorageMigrationExecutor {

    private static final Logger logger = LoggerFactory.getLogger(StorageMigrationExecutor.class);

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private final IMetaManager metaManager;

    private final ThreadPoolExecutor executor;

    private static class MigrationTask implements Callable<Boolean>, Runnable {

        private final ThreadPoolExecutor executor;

        private final IMetaManager metaManager;

        private final long storageId;

        private final boolean migrationData;

        public MigrationTask(ThreadPoolExecutor executor, IMetaManager metaManager, long storageId, boolean migrationData) {
            this.executor = executor;
            this.metaManager = metaManager;
            this.storageId = storageId;
            this.migrationData = migrationData;
        }

        @Override
        public Boolean call() {
            StorageMigrationPlan plan = metaManager.getStorageMigrationPlan(storageId);
            if (plan != null) {
                // 中继查询
                logger.info("this is a replay migration");
            } else {
                plan = MigrationManager.getInstance().getStorageMigration().generateMigrationPlans(storageId, migrationData);
                if (!metaManager.storeMigrationPlan(plan)) {
                    logger.info("storage migration plan " + plan + " failure");
                    return false;
                }
            }
            logger.info("[FaultTolerance][MigrationExecutor][iginx={}, id={}] data migration plan: {}", metaManager.getIginxId(), storageId, plan.getMigrationMap());

            Map<String, String> storageUnitMigrationMap = metaManager.startMigrationStorageUnits(plan.getMigrationMap(), plan.isMigrationData());
            if (storageUnitMigrationMap == null) {
                logger.info("start migration plan " + plan + " failure");
                return false;
            }
            List<Callable<Boolean>> tasks = new ArrayList<>();

            for (String sourceStorageUnitId: storageUnitMigrationMap.keySet()) {
                String targetStorageUnitId = storageUnitMigrationMap.get(sourceStorageUnitId);
                MigrationPolicy migrationPolicy = MigrationManager.getInstance().getMigration();
                tasks.add(() -> {
                    logger.info("[FaultTolerance][MigrationExecutor][iginx={}, id={}] migration from {} to {}, migrationData = {}", metaManager.getIginxId(), storageId, sourceStorageUnitId, targetStorageUnitId, migrationData);
                    if (migrationData) {
                        logger.info("call migration from {} to {}", sourceStorageUnitId, targetStorageUnitId);
                        if (!migrationPolicy.migrationData(sourceStorageUnitId, targetStorageUnitId)) {
                            return false;
                        }
                        return metaManager.finishMigrationStorageUnit(sourceStorageUnitId, true);
                    } else {
                        return migrationPolicy.migrationData(sourceStorageUnitId, targetStorageUnitId);
                    }
                });
            }

            try {
                List<Future<Boolean>> futures = executor.invokeAll(tasks);
                for(Future<Boolean> future: futures) {
                    Boolean ret = future.get();
                    if (ret == null || !ret) {
                        return false;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return false;
            }
            logger.info("[FaultTolerance][MigrationExecutor][iginx={}, id={}] all migration task finished", metaManager.getIginxId(), storageId);
            if (!migrationData) {
                for (String storageUnitId: plan.getMigrationMap().keySet()) {
                    metaManager.finishMigrationStorageUnit(storageUnitId, false);
                }
            }
            if (!metaManager.deleteMigrationPlan(storageId)) {
                return false;
            }
            StorageEngineMeta engineMeta = metaManager.getStorageEngine(storageId);
            engineMeta.setRemoved(true);
            logger.info("[FaultTolerance][MigrationExecutor][iginx={}, id={}] migration finished", metaManager.getIginxId(), storageId);
            return metaManager.updateStorageEngine(storageId, engineMeta);
        }

        @Override
        public void run() {
            logger.info("[FaultTolerance][MigrationExecutor][iginx={}, id={}] data migration start..", metaManager.getIginxId(), storageId);
            long start = System.currentTimeMillis();
            call();
            long span = System.currentTimeMillis() - start;
            logger.info("[FaultTolerance][MigrationExecutor][iginx={}, id={}] data migration finish, span = {}", metaManager.getIginxId(), storageId, span);
        }
    }

    private StorageMigrationExecutor() {
        metaManager = DefaultMetaManager.getInstance();
        executor = new ThreadPoolExecutor(ConfigDescriptor.getInstance().getConfig().getMigrationThreadPoolSize(),
                Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    }

    public static StorageMigrationExecutor getInstance() {
        return StorageMigrationExecutorHolder.INSTANCE;
    }

    public boolean migration(long storageId, boolean sync, boolean migrationData) {
        MigrationTask task = new MigrationTask(executor, metaManager, storageId, migrationData);
        if (sync) {
            return Boolean.TRUE.equals(task.call());
        }
        executor.execute(task);
        return true;
    }

    private static class StorageMigrationExecutorHolder {

        private static final StorageMigrationExecutor INSTANCE = new StorageMigrationExecutor();

        private StorageMigrationExecutorHolder() {
        }

    }

}
