/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.metadata.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class FragmentMeta {

  private String id;

  private final KeyInterval keyInterval;

  private final ColumnsInterval columnsInterval;

  private boolean isMaster;

  private String masterId;

  private String storageUnitId;

  private transient StorageUnitMeta storageUnit;

  private transient String fakeStorageUnitId;

  private long createdBy;

  private long updatedBy;

  private boolean initial = true;

  private boolean dummy = false;

  private boolean valid = true;

  // TODO AYZ 未保证一致性
  private transient List<FragmentMeta> replicas = new ArrayList<>();

  public FragmentMeta(
      String id,
      boolean isMaster,
      String masterId,
      KeyInterval keyInterval,
      ColumnsInterval columnsInterval) {
    this.id = id;
    this.isMaster = isMaster;
    this.masterId = masterId;
    this.keyInterval = keyInterval;
    this.columnsInterval = columnsInterval;
  }

  public FragmentMeta(
      String id,
      boolean isMaster,
      String masterId,
      String startColumn,
      String endColumn,
      long startKey,
      long endKey) {
    this(
        id,
        isMaster,
        masterId,
        new KeyInterval(startKey, endKey),
        new ColumnsInterval(startColumn, endColumn));
  }

  public FragmentMeta(
      String id,
      boolean isMaster,
      String masterId,
      String startColumn,
      String endColumn,
      long startKey,
      long endKey,
      String fakeStorageUnitId) {
    this(id, isMaster, masterId, startColumn, endColumn, startKey, endKey);
    this.fakeStorageUnitId = fakeStorageUnitId;
  }

  public FragmentMeta(
      String id,
      boolean isMaster,
      String masterId,
      ColumnsInterval columnsInterval,
      KeyInterval keyInterval,
      String fakeStorageUnitId) {
    this(id, isMaster, masterId, keyInterval, columnsInterval);
    this.fakeStorageUnitId = fakeStorageUnitId;
  }

  public FragmentMeta(
      String id,
      boolean isMaster,
      String masterId,
      String startColumn,
      String endColumn,
      long startKey,
      long endKey,
      StorageUnitMeta storageUnit) {
    this(id, isMaster, masterId, startColumn, endColumn, startKey, endKey);
    setStorageUnit(storageUnit);
  }

  public FragmentMeta(
      String id,
      boolean isMaster,
      String masterId,
      ColumnsInterval columnsInterval,
      KeyInterval keyInterval,
      StorageUnitMeta storageUnit) {
    this(id, isMaster, masterId, keyInterval, columnsInterval);
    setStorageUnit(storageUnit);
  }

  public FragmentMeta(FragmentMeta fragment) {
    this.id = fragment.id;
    this.keyInterval = fragment.keyInterval;
    this.columnsInterval = fragment.columnsInterval;
    this.isMaster = fragment.isMaster;
    this.masterId = fragment.masterId;
    this.storageUnitId = fragment.storageUnitId;
    this.createdBy = fragment.createdBy;
    this.updatedBy = fragment.updatedBy;
    this.initial = fragment.initial;
    this.dummy = fragment.dummy;
    this.valid = fragment.valid;
  }

  public FragmentMeta(
      String id,
      KeyInterval keyInterval,
      ColumnsInterval columnsInterval,
      boolean isMaster,
      String masterId,
      String storageUnitId,
      long createdBy,
      long updatedBy,
      boolean initial,
      boolean dummy,
      boolean valid) {
    this.id = id;
    this.keyInterval = keyInterval;
    this.columnsInterval = columnsInterval;
    this.isMaster = isMaster;
    this.masterId = masterId;
    this.storageUnitId = storageUnitId;
    this.createdBy = createdBy;
    this.updatedBy = updatedBy;
    this.initial = initial;
    this.dummy = dummy;
    this.valid = valid;
  }

  public FragmentMeta endFragmentMeta(long endKey) {
    FragmentMeta fragment =
        new FragmentMeta(
            id,
            isMaster,
            masterId,
            columnsInterval.getStartColumn(),
            columnsInterval.getEndColumn(),
            keyInterval.getStartKey(),
            endKey);
    fragment.setStorageUnit(storageUnit);
    fragment.setCreatedBy(createdBy);
    return fragment;
  }

  public FragmentMeta renameFragment(String id, String masterId, boolean initial) {
    FragmentMeta fragment = new FragmentMeta(id, isMaster, masterId, keyInterval, columnsInterval);
    fragment.setCreatedBy(createdBy);
    fragment.setInitial(initial);
    fragment.setReplicas(replicas);
    fragment.setStorageUnitId(storageUnitId);
    return fragment;
  }

  public synchronized void addReplica(FragmentMeta replica) {
    replicas.add(replica);
  }

  public synchronized void removeReplica(FragmentMeta replica) {
    replicas.remove(replica);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isMaster() {
    return isMaster;
  }

  public void setMaster(boolean master) {
    isMaster = master;
  }

  public String getMasterId() {
    return masterId;
  }

  public void setMasterId(String masterId) {
    this.masterId = masterId;
  }

  public String getStorageUnitId() {
    return storageUnitId;
  }

  public void setStorageUnitId(String storageUnitId) {
    this.storageUnitId = storageUnitId;
  }

  public StorageUnitMeta getStorageUnit() {
    return storageUnit;
  }

  public void setStorageUnit(StorageUnitMeta storageUnit) {
    this.storageUnit = storageUnit;
    this.storageUnitId = storageUnit.getId();
  }

  public String getFakeStorageUnitId() {
    return fakeStorageUnitId;
  }

  public void setFakeStorageUnitId(String fakeStorageUnitId) {
    this.fakeStorageUnitId = fakeStorageUnitId;
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

  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean valid) {
    this.valid = valid;
  }

  public List<FragmentMeta> getReplicas() {
    return replicas;
  }

  public void setReplicas(List<FragmentMeta> replicas) {
    this.replicas = replicas;
  }

  @Override
  public String toString() {
    return "FragmentMeta{"
        + "id='"
        + id
        + '\''
        + ", keyInterval="
        + keyInterval
        + ", columnsInterval="
        + columnsInterval
        + ", isMaster="
        + isMaster
        + ", masterId='"
        + masterId
        + '\''
        + ", storageUnitId='"
        + storageUnitId
        + '\''
        + ", createdBy="
        + createdBy
        + ", updatedBy="
        + updatedBy
        + ", initial="
        + initial
        + ", dummy="
        + dummy
        + ", valid="
        + valid
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FragmentMeta that = (FragmentMeta) o;
    return Objects.equals(id, that.id)
        && Objects.equals(keyInterval, that.keyInterval)
        && Objects.equals(columnsInterval, that.columnsInterval);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyInterval, columnsInterval);
  }

  public KeyInterval getKeyInterval() {
    return keyInterval;
  }

  public ColumnsInterval getColumnsInterval() {
    return columnsInterval;
  }

  public boolean isInitial() {
    return initial;
  }

  public void setInitial(boolean initial) {
    this.initial = initial;
  }

  public boolean isDummy() {
    return dummy;
  }

  public void setDummy(boolean dummy) {
    this.dummy = dummy;
  }

  public static long sizeOf() {
    return 62L;
  }
}
