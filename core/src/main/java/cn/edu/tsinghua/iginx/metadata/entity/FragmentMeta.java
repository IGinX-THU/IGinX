package cn.edu.tsinghua.iginx.metadata.entity;

import java.util.Objects;

public final class FragmentMeta {

  private final KeyInterval keyInterval;

  private final ColumnsInterval columnsInterval;

  private long createdBy;

  private long updatedBy;

  private String masterStorageUnitId;

  private transient StorageUnitMeta masterStorageUnit;

  private transient String fakeStorageUnitId;

  private boolean initialFragment = true;

  private boolean dummyFragment = false;

  private boolean valid = true;

  public FragmentMeta(String startPrefix, String endPrefix, long startKey, long endKey) {
    this.keyInterval = new KeyInterval(startKey, endKey);
    this.columnsInterval = new ColumnsInterval(startPrefix, endPrefix);
  }

  public FragmentMeta(
      String startPrefix, String endPrefix, long startKey, long endKey, String fakeStorageUnitId) {
    this.keyInterval = new KeyInterval(startKey, endKey);
    this.columnsInterval = new ColumnsInterval(startPrefix, endPrefix);
    this.fakeStorageUnitId = fakeStorageUnitId;
  }

  public FragmentMeta(
      ColumnsInterval columnsInterval, KeyInterval keyInterval, String fakeStorageUnitId) {
    this.keyInterval = keyInterval;
    this.columnsInterval = columnsInterval;
    this.fakeStorageUnitId = fakeStorageUnitId;
  }

  public FragmentMeta(
      String startPrefix,
      String endPrefix,
      long startKey,
      long endKey,
      StorageUnitMeta masterStorageUnit) {
    this.keyInterval = new KeyInterval(startKey, endKey);
    this.columnsInterval = new ColumnsInterval(startPrefix, endPrefix);
    this.masterStorageUnit = masterStorageUnit;
    this.masterStorageUnitId = masterStorageUnit.getMasterId();
  }

  public FragmentMeta(
      ColumnsInterval columnsInterval, KeyInterval keyInterval, StorageUnitMeta masterStorageUnit) {
    this.keyInterval = keyInterval;
    this.columnsInterval = columnsInterval;
    this.masterStorageUnit = masterStorageUnit;
    this.masterStorageUnitId = masterStorageUnit.getMasterId();
  }

  public FragmentMeta(
      KeyInterval keyInterval,
      ColumnsInterval columnsInterval,
      long createdBy,
      long updatedBy,
      String masterStorageUnitId,
      StorageUnitMeta masterStorageUnit,
      String fakeStorageUnitId,
      boolean initialFragment,
      boolean dummyFragment) {
    this.keyInterval = keyInterval;
    this.columnsInterval = columnsInterval;
    this.createdBy = createdBy;
    this.updatedBy = updatedBy;
    this.masterStorageUnitId = masterStorageUnitId;
    this.masterStorageUnit = masterStorageUnit;
    this.fakeStorageUnitId = fakeStorageUnitId;
    this.initialFragment = initialFragment;
    this.dummyFragment = dummyFragment;
  }

  public KeyInterval getKeyInterval() {
    return keyInterval;
  }

  public ColumnsInterval getColumnsInterval() {
    return columnsInterval;
  }

  public FragmentMeta endFragmentMeta(long endKey) {
    FragmentMeta fragment =
        new FragmentMeta(
            columnsInterval.getStartColumn(),
            columnsInterval.getEndColumn(),
            keyInterval.getStartKey(),
            endKey);
    fragment.setMasterStorageUnit(masterStorageUnit);
    fragment.setMasterStorageUnitId(masterStorageUnitId);
    fragment.setCreatedBy(createdBy);
    fragment.setInitialFragment(initialFragment);
    return fragment;
  }

  public long getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(long createdBy) {
    this.createdBy = createdBy;
  }

  public long getUpdatedBy() {
    return updatedBy;
  }

  public void setUpdatedBy(long updatedBy) {
    this.updatedBy = updatedBy;
  }

  public StorageUnitMeta getMasterStorageUnit() {
    return masterStorageUnit;
  }

  public void setMasterStorageUnit(StorageUnitMeta masterStorageUnit) {
    this.masterStorageUnit = masterStorageUnit;
    this.masterStorageUnitId = masterStorageUnit.getMasterId();
  }

  public String getFakeStorageUnitId() {
    return fakeStorageUnitId;
  }

  public void setFakeStorageUnitId(String fakeStorageUnitId) {
    this.fakeStorageUnitId = fakeStorageUnitId;
  }

  public String getMasterStorageUnitId() {
    return masterStorageUnitId;
  }

  public void setMasterStorageUnitId(String masterStorageUnitId) {
    this.masterStorageUnitId = masterStorageUnitId;
  }

  public boolean isValid() {
    return valid;
  }

  public void setIfValid(boolean ifValid) {
    this.valid = ifValid;
  }

  @Override
  public String toString() {
    return "FragmentMeta{"
        + "keyInterval="
        + keyInterval
        + ", columnsInterval="
        + columnsInterval
        + ", createdBy="
        + createdBy
        + ", updatedBy="
        + updatedBy
        + ", masterStorageUnitId='"
        + masterStorageUnitId
        + '\''
        + ", dummyFragment="
        + dummyFragment
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FragmentMeta that = (FragmentMeta) o;
    return Objects.equals(keyInterval, that.keyInterval)
        && Objects.equals(columnsInterval, that.columnsInterval);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyInterval, columnsInterval);
  }

  public boolean isInitialFragment() {
    return initialFragment;
  }

  public void setInitialFragment(boolean initialFragment) {
    this.initialFragment = initialFragment;
  }

  public boolean isDummyFragment() {
    return dummyFragment;
  }

  public void setDummyFragment(boolean dummyFragment) {
    this.dummyFragment = dummyFragment;
  }

  public static long sizeOf() {
    return 62L;
  }
}
