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
package cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.proposal.content.LossConnectionProposalContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.proposal.content.RestoreConnectionProposalContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.content.LossConnectionVoteContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.content.RestoreConnectionVoteContent;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.listener.LossConnectionVoteListener;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.vote.listener.RestoreConnectionVoteListener;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.SyncVote;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.NetworkException;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.VoteExpiredException;
import cn.edu.tsinghua.iginx.migration.storage.StorageMigrationExecutor;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import cn.hutool.core.collection.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConnectionManager {

    private static final String LOSS_CONNECTION = "loss_connection";

    private static final String RESTORE_CONNECTION = "restore_connection";

    private static final String PROPOSAL_KEY = "storage_%d";

    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private static final Random random = new Random();

    private final IMetaManager iMetaManager;

    private final ReadWriteLock lock;

    private final Map<Long, Connector> connectors;

    private final Set<Long> blockedStorages;

    private final Set<Long> removedStorages;

    private final Set<Long> inVotes;

    private final Set<Long> updatedConnectors;

    private final ScheduledExecutorService scheduledService;

    private final ProposalListener lossConnectionListener = new ProposalListener() {

        @Override
        public void onCreate(String key, SyncProposal proposal) {
            LossConnectionProposalContent content = JsonUtils.fromJson(proposal.getContent(), LossConnectionProposalContent.class);
            logger.info("[FaultTolerance][ConnectionManager][ProposalListener][iginx={}, id={}, key={}] receive loss connection proposal {}, now = {}", iMetaManager.getIginxId(), content.getId(), key, new String(JsonUtils.toJson(proposal)), System.currentTimeMillis());
            long id = content.getId();
            inVotes.add(id);

            // test connection access able:
            checkAndVoteForLossConnection(key, id);
        }

        @Override
        public void onUpdate(String key, SyncProposal before, SyncProposal after) {
            LossConnectionProposalContent content = JsonUtils.fromJson(after.getContent(), LossConnectionProposalContent.class);
            logger.info("[FaultTolerance][ConnectionManager][ProposalListener][iginx={}, id={}, key={}] receive loss connection proposal {}, now = {}", iMetaManager.getIginxId(), content.getId(), key, new String(JsonUtils.toJson(after)), System.currentTimeMillis());
            long id = content.getId();
            if (!content.isAlive()) {
                blockedStorages.add(id);
            }
            inVotes.remove(id);
            if (content.isAlive()) {
                return;
            }
            logger.info("[FaultTolerance][ConnectionManager][ZiYuan][ProposalListener][iginx={}, id={}, key={}] decide remove and block storage engine, now = {}", iMetaManager.getIginxId(), content.getId(), key, System.currentTimeMillis());
            IStorageWrapper wrapper = (IStorageWrapper) StoragePhysicalTaskExecutor.getInstance().getStorageManager().getStorage(id).k;
            wrapper.setBlocked(true);
            // 开启一个迁移任务，把这个数据给迁移走，这里可能会 race，但是总有一个会成功，对外话术就是，所有的节点都会尝试开启
            if (after.getProposer() == iMetaManager.getIginxId()) {
                logger.info("[FaultTolerance][ConnectionManager][ProposalListener][iginx={}, id={}, key={}] submit migration task, now = {}", iMetaManager.getIginxId(), content.getId(), key, System.currentTimeMillis());
                StorageMigrationExecutor.getInstance().migration(content.getId(), false, false);
            }
        }
    };

    private final ProposalListener restoreConnectionListener = new ProposalListener() {
        @Override
        public void onCreate(String key, SyncProposal proposal) {
            logger.info("receive proposal(key = " + key + ") create for restore connection: " + new String(JsonUtils.toJson(proposal)));
            RestoreConnectionProposalContent content = JsonUtils.fromJson(proposal.getContent(), RestoreConnectionProposalContent.class);

            long id = content.getId();
            inVotes.add(id);

            // test connection access able:
            checkAndVoteForRestoreConnection(key, id);
        }

        @Override
        public void onUpdate(String key, SyncProposal before, SyncProposal after) {
            logger.info("receive proposal(key = " + key + ") update for loss connection: " + new String(JsonUtils.toJson(after)));
            RestoreConnectionProposalContent content = JsonUtils.fromJson(after.getContent(), RestoreConnectionProposalContent.class);
            long id = content.getId();
            inVotes.remove(id);
            if (!content.isAlive()) {
                logger.info("storage " + id + " still not alive!");
                return;
            }
            blockedStorages.remove(id);
            // TODO: storage engine will be remove from cluster
            logger.info("resume storage " + id + " from cluster");
            IStorageWrapper wrapper = (IStorageWrapper) StoragePhysicalTaskExecutor.getInstance().getStorageManager().getStorage(id).k;
            wrapper.setBlocked(false);
        }
    };

    private SyncProtocol lossConnectionProtocol;

    private SyncProtocol restoreConnectionProtocol;

    private ConnectionManager() {
        iMetaManager = DefaultMetaManager.getInstance();
        this.initProtocols();
        lock = new ReentrantReadWriteLock();
        blockedStorages = new ConcurrentHashSet<>();
        removedStorages = new ConcurrentHashSet<>();
        inVotes = new ConcurrentHashSet<>();
        connectors = new HashMap<>();
        updatedConnectors = new HashSet<>();
        scheduledService = new ScheduledThreadPoolExecutor(ConfigDescriptor.getInstance().getConfig().getStorageHeartbeatThresholdPoolSize());
    }

    private void initProtocols() {
        try {
            iMetaManager.initProtocol(LOSS_CONNECTION);
            lossConnectionProtocol = iMetaManager.getProtocol(LOSS_CONNECTION);
            lossConnectionProtocol.registerProposalListener(lossConnectionListener);
            iMetaManager.initProtocol(RESTORE_CONNECTION);
            restoreConnectionProtocol = iMetaManager.getProtocol(RESTORE_CONNECTION);
            restoreConnectionProtocol.registerProposalListener(restoreConnectionListener);
        } catch (Exception e) {
            logger.error("init protocol failure: ", e);
            System.exit(-1);
        }
    }

    public void registerConnector(long id, Connector connector) {
        if (connector == null) {
            throw new IllegalArgumentException("connector for storage{id=" + id + "} shouldn't be null");
        }
        lock.writeLock().lock();
        boolean alreadyExists = connectors.containsKey(id);
        if (alreadyExists) {
            updatedConnectors.add(id);
        } else {
            scheduledService.scheduleWithFixedDelay(new HeartbeatTask(id, connector),
                    (long)(ConfigDescriptor.getInstance().getConfig().getStorageHeartbeatInterval() * random.nextDouble()), ConfigDescriptor.getInstance().getConfig().getStorageHeartbeatInterval(), TimeUnit.MILLISECONDS);
        }
        connectors.put(id, connector);
        lock.writeLock().unlock();
    }

    public void removeConnector(long id) {
        removedStorages.add(id);
    }

    private void checkAndVoteForLossConnection(String key, long id) {
        scheduledService.submit(() -> {
            LossConnectionVoteContent content = new LossConnectionVoteContent(checkConnection(id));
            logger.info("[checkAndVoteForLossConnection] async check connection for " + id + ", is alive? " + content.isAlive());
            try {
                lossConnectionProtocol.voteFor(key, new SyncVote(iMetaManager.getIginxId(), JsonUtils.toJson(content)));
            } catch (NetworkException e) {
                logger.error("[checkAndVoteForLossConnection] vote for " + id + " failure: ", e);
            } catch (VoteExpiredException e) {
                logger.error("[checkAndVoteForLossConnection] vote for " + id + " expired: ", e);
            }
        });
    }

    private void checkAndVoteForRestoreConnection(String key, long id) {
        scheduledService.submit(() -> {
            RestoreConnectionVoteContent content = new RestoreConnectionVoteContent(checkConnection(id));
            logger.info("[checkAndVoteForRestoreConnection] async check connection for " + id + ", is alive? " + content.isAlive());
            try {
                restoreConnectionProtocol.voteFor(key, new SyncVote(iMetaManager.getIginxId(), JsonUtils.toJson(content)));
            } catch (NetworkException e) {
                logger.error("[checkAndVoteForRestoreConnection] vote for " + id + " failure: ", e);
            } catch (VoteExpiredException e) {
                logger.error("[checkAndVoteForRestoreConnection] vote for " + id + " expired: ", e);
            }
        });
    }

    private boolean checkConnection(long id) {
        this.lock.readLock().lock();
        Connector connector = connectors.get(id);
        this.lock.readLock().unlock();
        if (connector == null) {
            return false;
        }
        return connector.echo(ConfigDescriptor.getInstance().getConfig().getStorageHeartbeatTimeout(), TimeUnit.MILLISECONDS);
    }

    private class HeartbeatTask implements Runnable {

        private final long id;

        private Connector connector;

        private final double restoreConnectionProbability = ConfigDescriptor.getInstance().getConfig().getStorageRestoreHeartbeatProbability();

        public HeartbeatTask(long id, Connector connector) {
            this.id = id;
            this.connector = connector;
        }

        @Override
        public void run() {
            long iginxId = iMetaManager.getIginxId();
            if (iginxId != 0L) {
                return;
            }
            if (removedStorages.contains(id)) {
                //logger.info("storage {} has been removed, we doesn't need to check alive.", id);
                return;
            }
            //logger.info("scheduled test connection for " + id);
            if (inVotes.contains(id)) {
                //logger.info("don't need to check connection for " + id + ", because it is in vote!");
                return;
            }
            boolean block = blockedStorages.contains(id);
            if (block) {
                if (random.nextDouble() > restoreConnectionProbability) {
                    //logger.info("don't need to check connection for " + id);
                    return;
                }
                //logger.info("try restore connection for " + id + " timely");
            }

            //logger.info("[FaultTolerance][ConnectionManager][iginx={}, id={}] check storage alive..., now = {}", iginxId, id, System.currentTimeMillis());
            ConnectionManager manager = ConnectionManager.this;
            int maxRetryTimes = ConfigDescriptor.getInstance().getConfig().getStorageHeartbeatMaxRetryTimes();
            long heartbeatTimeout = ConfigDescriptor.getInstance().getConfig().getStorageHeartbeatTimeout();

            boolean updated;
            manager.lock.readLock().lock();
            updated = manager.updatedConnectors.contains(id);
            manager.lock.readLock().unlock();
            if (updated) {
                Connector newConnector;
                manager.lock.writeLock().lock();
                manager.updatedConnectors.remove(id);
                newConnector = manager.connectors.get(id);
                manager.lock.writeLock().unlock();
                connector.reset();
                connector = newConnector;
            }
            for (int i = 0; i < maxRetryTimes; i++) {
                if (connector.echo(heartbeatTimeout, TimeUnit.MILLISECONDS)) {
                    //logger.info("not loss connection for " + id + ", curr timestamp = " + System.currentTimeMillis());
                    if (block) {
                        // will not reach this codes
                        // start proposal for restore storage status
                        SyncProposal proposal = new SyncProposal(iMetaManager.getIginxId(), JsonUtils.toJson(new RestoreConnectionProposalContent(id)));
                        boolean success;
                        try {
                            success = restoreConnectionProtocol.startProposal(String.format(PROPOSAL_KEY, id), proposal, new RestoreConnectionVoteListener(iMetaManager.getIginxClusterSize(), proposal, restoreConnectionProtocol));
                        } catch (NetworkException e) {
                            logger.error("start restore connection proposal for " + id + " failure: ", e);
                            return;
                        }
                        if (!success) {
                            logger.warn("start restore connection proposal for " + id + " failure, due to race!");
                            return;
                        }
                    }
                    //logger.info("[FaultTolerance][ConnectionManager][iginx={}, id={}] storage still alive", iginxId, id);
                    return;
                }
            }
            if (block) {
                return;
            }
            logger.info("[FaultTolerance][ConnectionManager][ZiYuan][iginx={}, id={}] storage loss connection, now = {}", iginxId, id, System.currentTimeMillis());

            // start proposal for check storage status
            SyncProposal proposal = new SyncProposal(iMetaManager.getIginxId(), JsonUtils.toJson(new LossConnectionProposalContent(id)));
            boolean success;
            try {
                success = lossConnectionProtocol.startProposal(String.format(PROPOSAL_KEY, id), proposal, new LossConnectionVoteListener(iMetaManager.getIginxClusterSize(), proposal, lossConnectionProtocol));
            } catch (NetworkException e) {
                logger.error("[FaultTolerance][ConnectionManager][iginx={}, id={}] start loss connection proposal error: ", iginxId, id, e);
                return;
            }
            if (!success) {
                logger.error("[FaultTolerance][ConnectionManager][iginx={}, id={}] start loss connection proposal error, due to race", iginxId, id);
                return;
            }
            logger.error("[FaultTolerance][ConnectionManager][iginx={}, id={}] start loss connection proposal success ", iginxId, id);
        }
    }

    public static ConnectionManager getInstance() {
        return ConnectionManagerInstanceHolder.INSTANCE;
    }

    private static class ConnectionManagerInstanceHolder {

        private static final ConnectionManager INSTANCE = new ConnectionManager();

    }

}
