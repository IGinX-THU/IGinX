package cn.edu.tsinghua.iginx.migration.storage;

import java.util.Map;

public class StorageMigrationPlan {

    private final boolean migrationData;

    private final long migrationId;

    private final Map<String, Long> migrationMap;

    private long migrationOwner;

    public StorageMigrationPlan(long migrationId, boolean migrationData, Map<String, Long> migrationMap, long migrationOwner) {
        this.migrationId = migrationId;
        this.migrationData = migrationData;
        this.migrationMap = migrationMap;
        this.migrationOwner = migrationOwner;
    }

    public boolean isMigrationData() {
        return migrationData;
    }

    public long getMigrationId() {
        return migrationId;
    }

    public Map<String, Long> getMigrationMap() {
        return migrationMap;
    }

    public long getMigrationOwner() {
        return migrationOwner;
    }

    public void setMigrationOwner(long migrationOwner) {
        this.migrationOwner = migrationOwner;
    }

    @Override
    public String toString() {
        return "StorageMigrationPlan{" +
                "migrationData=" + migrationData +
                ", migrationId=" + migrationId +
                ", migrationMap=" + migrationMap +
                ", migrationOwner=" + migrationOwner +
                '}';
    }
}
