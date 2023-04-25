package cn.edu.tsinghua.iginx.migration.storage;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;

public abstract class StorageMigrationPolicy {

    protected final IMetaManager metaManager;

    public StorageMigrationPolicy(IMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public abstract StorageMigrationPlan generateMigrationPlans(long storageId, boolean migrationData);
}
