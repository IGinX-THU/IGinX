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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

import java.util.Collections;
import java.util.List;

public class StoragePhysicalTask extends AbstractPhysicalTask {

    private final FragmentMeta targetFragment;
    private final boolean sync;
    private final boolean needBroadcasting;
    private String storageUnit;
    private long storage;
    private boolean dummyStorageUnit;

    private String[] backupStorageUnits;

    private long[] backupStorages;

    private int nextIndex;

    private boolean skipData = false;

    private long lastTimestamp = 0L;

    public StoragePhysicalTask(List<Operator> operators, RequestContext context) {
        this(operators, context, ((FragmentSource) ((UnaryOperator) operators.get(0)).getSource()).getFragment(), true, false);
    }

    public StoragePhysicalTask(List<Operator> operators, RequestContext context, boolean sync, boolean needBroadcasting) {
        this(operators, context, ((FragmentSource) ((UnaryOperator) operators.get(0)).getSource()).getFragment(), sync, needBroadcasting);
    }

    public StoragePhysicalTask(List<Operator> operators, RequestContext context, FragmentMeta targetFragment, boolean sync, boolean needBroadcasting) {
        super(TaskType.Storage, operators, context);
        this.targetFragment = targetFragment;
        this.sync = sync;
        this.needBroadcasting = needBroadcasting;
    }

    public StoragePhysicalTask(List<Operator> operators, RequestContext context, boolean forTest) {
        super(TaskType.Storage, operators, context);
        this.targetFragment = null;
        this.sync = true;
        this.needBroadcasting = false;
    }

    public FragmentMeta getTargetFragment() {
        return targetFragment;
    }

    public String getStorageUnit() {
        return storageUnit;
    }

    public long getStorage() {
        return storage;
    }

    public void setStorageUnit(String storageUnit) {
        this.storageUnit = storageUnit;
    }

    public boolean isDummyStorageUnit() {
        return dummyStorageUnit;
    }

    public void setDummyStorageUnit(boolean dummyStorageUnit) {
        this.dummyStorageUnit = dummyStorageUnit;
    }

    public boolean isSync() {
        return sync;
    }

    public boolean isNeedBroadcasting() {
        return needBroadcasting;
    }

    public void setBackup(long[] backupStorages, String[] backupStorageUnits) {
        this.backupStorages = backupStorages;
        this.backupStorageUnits = backupStorageUnits;
    }

    public boolean canBackUp() {
        return getOperators().get(0).getType() == OperatorType.Project;
    }

    public boolean hasBackup() {
        return canBackUp() && backupStorageUnits != null && nextIndex < backupStorageUnits.length;
    }

    public void backUp() {
        backUp(0L);
    }

    public void backUp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
        if (lastTimestamp > 0) {
            this.skipData = true;
        }
        storage = backupStorages[nextIndex];
        storageUnit = backupStorageUnits[nextIndex];
        nextIndex++;
    }
    public boolean isSkipData() {
        return skipData;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    @Override
    public String toString() {
        return "StoragePhysicalTask{" +
            "targetFragment=" + targetFragment +
            ", storageUnit='" + storageUnit +
            '}';
    }

    @Override
    public boolean hasParentTask() {
        return false;
    }

    @Override
    public List<PhysicalTask> getParentTasks() {
        return Collections.emptyList();
    }
}
