package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.migration.storage.StorageMigrationPlan;
import cn.edu.tsinghua.iginx.policy.simple.ColumnCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaManagerMock implements IMetaManager {
    private final List<FragmentMeta> fragments = new ArrayList<>();

    @Override
    public boolean addStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
        return false;
    }

    @Override
    public boolean storeMigrationPlan(StorageMigrationPlan plan) {
        return false;
    }

    @Override
    public List<StorageMigrationPlan> scanStorageMigrationPlan() {
        return null;
    }

    @Override
    public StorageMigrationPlan getStorageMigrationPlan(long storageId) {
        return null;
    }

    @Override
    public boolean transferMigrationPlan(long id, long from, long to) {
        return false;
    }

    @Override
    public boolean deleteMigrationPlan(long id) {
        return false;
    }

    @Override
    public Map<String, String> startMigrationStorageUnits(
            Map<String, Long> migrationMap, boolean migrationData) {
        return null;
    }

    @Override
    public boolean finishMigrationStorageUnit(String storageUnitId, boolean migrationData) {
        return false;
    }

    @Override
    public boolean updateStorageUnit(StorageUnitMeta storageUnit) {
        return false;
    }

    @Override
    public boolean updateStorageEngine(long storageID, StorageEngineMeta storageEngineMeta) {
        return false;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return null;
    }

    @Override
    public List<StorageEngineMeta> getWriteableStorageEngineList() {
        return null;
    }

    @Override
    public int getStorageEngineNum() {
        return 0;
    }

    @Override
    public StorageEngineMeta getStorageEngine(long id) {
        return null;
    }

    @Override
    public StorageUnitMeta getStorageUnit(String id) {
        return null;
    }

    @Override
    public Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids) {
        return null;
    }

    @Override
    public List<StorageUnitMeta> getStorageUnits() {
        return null;
    }

    @Override
    public List<IginxMeta> getIginxList() {
        return null;
    }

    @Override
    public long getIginxId() {
        return 0;
    }

    @Override
    public List<FragmentMeta> getFragments() {
        return fragments;
    }

    @Override
    public List<FragmentMeta> getFragmentsByStorageUnit(String storageUnitId) {
        return null;
    }

    @Override
    public Pair<ColumnsRange, KeyInterval> getBoundaryOfStorageUnit(String storageUnitId) {
        return null;
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsRange(
            ColumnsRange columnsRange) {
        return null;
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsRange(
            ColumnsRange columnsRange, boolean withDummyFragment) {
        return null;
    }

    @Override
    public boolean hasDummyFragment(ColumnsRange columnsRange) {
        return false;
    }

    @Override
    public Map<ColumnsRange, FragmentMeta> getLatestFragmentMapByColumnsRange(
            ColumnsRange columnsRange) {
        return null;
    }

    @Override
    public Map<ColumnsRange, FragmentMeta> getLatestFragmentMap() {
        return null;
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
            ColumnsRange columnsRange, KeyInterval keyInterval) {
        return null;
    }

    @Override
    public Map<ColumnsRange, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
            ColumnsRange columnsRange, KeyInterval keyInterval, boolean withDummyFragment) {
        return null;
    }

    @Override
    public List<FragmentMeta> getFragmentListByColumnName(String colName) {
        return null;
    }

    @Override
    public FragmentMeta getLatestFragmentByColumnName(String colName) {
        return null;
    }

    @Override
    public List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
            String colName, KeyInterval keyInterval) {
        return null;
    }

    @Override
    public boolean createFragmentsAndStorageUnits(
            List<StorageUnitMeta> storageUnits, List<FragmentMeta> fragments) {
        return false;
    }

    @Override
    public FragmentMeta splitFragmentAndStorageUnit(
            StorageUnitMeta toAddStorageUnit, FragmentMeta toAddFragment, FragmentMeta fragment) {
        return null;
    }

    @Override
    public boolean hasFragment() {
        return false;
    }

    @Override
    public boolean createInitialFragmentsAndStorageUnits(
            List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments) {
        return false;
    }

    @Override
    public StorageUnitMeta generateNewStorageUnitMetaByFragment(
            FragmentMeta fragmentMeta, long targetStorageId) throws MetaStorageException {
        return null;
    }

    @Override
    public List<Long> selectStorageEngineIdList() {
        return null;
    }

    @Override
    public void registerStorageEngineChangeHook(StorageEngineChangeHook hook) {}

    @Override
    public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {}

    @Override
    public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {}

    @Override
    public Map<String, Integer> getSchemaMapping(String schema) {
        return null;
    }

    @Override
    public int getSchemaMappingItem(String schema, String key) {
        return 0;
    }

    @Override
    public boolean addUser(UserMeta user) {
        return false;
    }

    @Override
    public boolean updateUser(String username, String password, Set<AuthType> auths) {
        return false;
    }

    @Override
    public boolean removeUser(String username) {
        return false;
    }

    @Override
    public UserMeta getUser(String username) {
        return null;
    }

    @Override
    public List<UserMeta> getUsers() {
        return null;
    }

    @Override
    public List<UserMeta> getUsers(List<String> username) {
        return null;
    }

    @Override
    public void registerStorageUnitHook(StorageUnitHook hook) {}

    @Override
    public boolean election() {
        return false;
    }

    @Override
    public void saveColumnsData(InsertStatement statement) {}

    @Override
    public List<ColumnCalDO> getMaxValueFromColumns() {
        return null;
    }

    @Override
    public Map<String, Double> getColumnsData() {
        return null;
    }

    @Override
    public int updateVersion() {
        return 0;
    }

    @Override
    public Map<Integer, Integer> getColumnsVersionMap() {
        return null;
    }

    @Override
    public boolean addTransformTask(TransformTaskMeta transformTask) {
        return false;
    }

    @Override
    public boolean updateTransformTask(TransformTaskMeta transformTask) {
        return false;
    }

    @Override
    public boolean dropTransformTask(String name) {
        return false;
    }

    @Override
    public TransformTaskMeta getTransformTask(String name) {
        return null;
    }

    @Override
    public List<TransformTaskMeta> getTransformTasks() {
        return null;
    }

    @Override
    public void updateFragmentRequests(
            Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap)
            throws Exception {}

    @Override
    public void updateFragmentHeat(
            Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap)
            throws Exception {}

    @Override
    public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat()
            throws Exception {
        return null;
    }

    @Override
    public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) {}

    @Override
    public Map<FragmentMeta, Long> loadFragmentPoints() throws Exception {
        return null;
    }

    @Override
    public void clearMonitors() {}

    @Override
    public void removeFragment(FragmentMeta fragmentMeta) {}

    @Override
    public void addFragment(FragmentMeta fragmentMeta) {
        fragments.add(fragmentMeta);
    }

    @Override
    public void endFragmentByColumnsRange(FragmentMeta fragmentMeta, String endColumn) {}

    @Override
    public void updateFragmentByColumnsRange(
            ColumnsRange columnsRange, FragmentMeta fragmentMeta) {}

    @Override
    public void updateMaxActiveEndKey(long endKey) {}

    @Override
    public long getMaxActiveEndKey() {
        return 0;
    }

    @Override
    public void submitMaxActiveEndKey() {}
}
