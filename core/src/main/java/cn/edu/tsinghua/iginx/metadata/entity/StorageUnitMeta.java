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
package cn.edu.tsinghua.iginx.metadata.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class StorageUnitMeta {

  private String id;

  private long storageEngineId;

  private String masterId;

  private boolean isMaster;

  private long createdBy;

  private boolean initialStorageUnit = true;

  private boolean dummy = false;

  private boolean ifValid = true;

  private transient List<StorageUnitMeta> replicas = new ArrayList<>();

  public StorageUnitMeta(String id, long storageEngineId, String masterId, boolean isMaster) {
    this.id = id;
    this.storageEngineId = storageEngineId;
    this.masterId = masterId;
    this.isMaster = isMaster;
  }

  public StorageUnitMeta(String id, long storageEngineId) {
    this.id = id;
    this.storageEngineId = storageEngineId;
    this.masterId = id;
    this.isMaster = true;
    this.dummy = true;
    this.replicas = Collections.emptyList();
  }

  public StorageUnitMeta(
      String id,
      long storageEngineId,
      String masterId,
      boolean isMaster,
      boolean initialStorageUnit) {
    this.id = id;
    this.storageEngineId = storageEngineId;
    this.masterId = masterId;
    this.isMaster = isMaster;
    this.initialStorageUnit = initialStorageUnit;
  }

  public StorageUnitMeta(
      String id,
      long storageEngineId,
      String masterId,
      boolean isMaster,
      long createdBy,
      boolean initialStorageUnit,
      boolean dummy,
      List<StorageUnitMeta> replicas) {
    this.id = id;
    this.storageEngineId = storageEngineId;
    this.masterId = masterId;
    this.isMaster = isMaster;
    this.createdBy = createdBy;
    this.initialStorageUnit = initialStorageUnit;
    this.dummy = dummy;
    this.replicas = replicas;
  }

  public void addReplica(StorageUnitMeta storageUnit) {
    if (replicas == null) replicas = new ArrayList<>();
    replicas.add(storageUnit);
  }

  public void removeReplica(StorageUnitMeta storageUnit) {
    if (replicas == null) replicas = new ArrayList<>();
    replicas.remove(storageUnit);
  }

  public StorageUnitMeta renameStorageUnitMeta(String id, String masterId) {
    StorageUnitMeta storageUnitMeta = new StorageUnitMeta(id, storageEngineId, masterId, isMaster);
    storageUnitMeta.setCreatedBy(createdBy);
    storageUnitMeta.setInitialStorageUnit(initialStorageUnit);
    return storageUnitMeta;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getStorageEngineId() {
    return storageEngineId;
  }

  public void setStorageEngineId(long storageEngineId) {
    this.storageEngineId = storageEngineId;
  }

  public String getMasterId() {
    return masterId;
  }

  public void setMasterId(String masterId) {
    this.masterId = masterId;
  }

  public boolean isMaster() {
    return isMaster;
  }

  public void setMaster(boolean master) {
    isMaster = master;
  }

  public long getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(long createdBy) {
    this.createdBy = createdBy;
  }

  public boolean isInitialStorageUnit() {
    return initialStorageUnit;
  }

  public void setInitialStorageUnit(boolean initialStorageUnit) {
    this.initialStorageUnit = initialStorageUnit;
  }

  public boolean isDummy() {
    return dummy;
  }

  public void setDummy(boolean dummy) {
    this.dummy = dummy;
  }

  public boolean isIfValid() {
    return ifValid;
  }

  public void setIfValid(boolean ifValid) {
    this.ifValid = ifValid;
  }

  public List<StorageUnitMeta> getReplicas() {
    if (replicas == null) {
      replicas = new ArrayList<>();
    }
    return replicas;
  }

  public void setReplicas(List<StorageUnitMeta> replicas) {
    this.replicas = replicas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageUnitMeta that = (StorageUnitMeta) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("StorageUnitMeta: { id = ");
    builder.append(id);
    builder.append(", storageEngineId = ");
    builder.append(storageEngineId);
    builder.append(", masterId = ");
    builder.append(masterId);
    builder.append(", isMaster = ");
    builder.append(isMaster);
    builder.append(", createdBy = ");
    builder.append(createdBy);
    if (replicas != null) {
      builder.append(", replica id list = ");
      for (StorageUnitMeta storageUnit : replicas) {
        builder.append(" ");
        builder.append(storageUnit.getId());
      }
    }
    builder.append("}");
    return builder.toString();
  }
}
