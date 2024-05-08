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

public final class StorageUnitMeta {

  private String id;

  private long storageEngineId;

  private long createdBy;

  private boolean initial = true;

  private boolean dummy = false;

  private boolean valid = true;

  // TODO AYZ 未保证一致性
  private transient List<FragmentMeta> fragments = new ArrayList<>();

  public StorageUnitMeta(String id, long storageEngineId) {
    this.id = id;
    this.storageEngineId = storageEngineId;
  }

  public StorageUnitMeta(String id, long storageEngineId, boolean initial) {
    this(id, storageEngineId);
    this.initial = initial;
  }

  public StorageUnitMeta renameStorageUnitMeta(String id, boolean initialStorageUnit) {
    StorageUnitMeta storageUnitMeta = new StorageUnitMeta(id, storageEngineId);
    storageUnitMeta.setCreatedBy(createdBy);
    storageUnitMeta.setInitial(initialStorageUnit);
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

  public long getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(long createdBy) {
    this.createdBy = createdBy;
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

  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean valid) {
    this.valid = valid;
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
    builder.append(", createdBy = ");
    builder.append(createdBy);
    builder.append("}");
    return builder.toString();
  }
}
