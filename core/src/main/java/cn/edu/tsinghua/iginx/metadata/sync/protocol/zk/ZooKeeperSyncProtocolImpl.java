/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.metadata.sync.protocol.zk;

import cn.edu.tsinghua.iginx.metadata.sync.proposal.ProposalListener;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.SyncProposal;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.SyncVote;
import cn.edu.tsinghua.iginx.metadata.sync.proposal.VoteListener;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.ExecutionException;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.NetworkException;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.VoteExpiredException;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSyncProtocolImpl implements SyncProtocol {

  private static final long MAX_NETWORK_LATENCY = 30000;

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperSyncProtocolImpl.class);

  private static final String PATH_SEPARATOR = "/";

  private static final String PROTOCOL_PREFIX = "/protocol";

  private static final String PROTOCOL_LOCK = "/lock" + PROTOCOL_PREFIX;

  private static final String PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE = PROTOCOL_PREFIX + "/%s";

  private static final String PROTOCOL_PROPOSAL_TEMPLATE =
      PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE + "/%s";

  private static final String PROTOCOL_PROPOSAL_LOCK_TEMPLATE = PROTOCOL_LOCK + "/%s/%s";

  private static final String VOTE_PREFIX = "/vote";

  private static final String VOTE_PROPOSAL_CONTAINER_TEMPLATE = VOTE_PREFIX + "/%s";

  private static final String VOTE_PROPOSAL_TEMPLATE = VOTE_PROPOSAL_CONTAINER_TEMPLATE + "/%s";

  private final CuratorFramework client;

  private final String category;

  private ProposalListener listener;

  private TreeCache proposalCache;

  private TreeCache voteCache;

  private final Map<String, Long> latestProposalTimes;
  private final Map<String, VoteListener> voteListeners;

  private final ReadWriteLock proposalLock;

  public ZooKeeperSyncProtocolImpl(String category, CuratorFramework client)
      throws NetworkException {
    this(category, client, null);
  }

  public ZooKeeperSyncProtocolImpl(
      String category, CuratorFramework client, ProposalListener listener) throws NetworkException {
    this.client = client;
    this.category = category;
    this.initProtocol();
    this.listener = listener;
    this.voteListeners = new HashMap<>();
    this.proposalLock = new ReentrantReadWriteLock();
    this.latestProposalTimes = new HashMap<>();
    this.registerProposalListener();
    this.registerGlobalVoteListener();
  }

  private void initProtocol() throws NetworkException {
    try {
      this.client.createContainers(String.format(PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE, category));
      this.client.createContainers(String.format(VOTE_PROPOSAL_CONTAINER_TEMPLATE, category));
    } catch (Exception e) {
      LOGGER.error("init protocol container for {} failure: ", category, e);
      throw new NetworkException("init protocol container for " + category + "failure", e);
    }
  }

  private void registerProposalListener() throws NetworkException {
    this.proposalCache =
        new TreeCache(client, String.format(PROTOCOL_PROPOSAL_CONTAINER_TEMPLATE, category));
    this.proposalCache
        .getListenable()
        .addListener(
            (curatorFramework, event) -> {
              if (listener == null) {
                return;
              }
              if (event.getData() == null
                  || event.getData().getPath() == null
                  || !event.getData().getPath().contains("proposal")) {
                return;
              }
              LOGGER.info(
                  "receive event: path = "
                      + event.getData().getPath()
                      + ", type = "
                      + event.getType());
              try {
                String[] parts = event.getData().getPath().split("/");
                String key = parts[3];
                long createTime = Long.parseLong(parts[4].split("_")[1]);
                switch (event.getType()) {
                  case NODE_ADDED:
                    proposalLock.writeLock().lock();
                    latestProposalTimes.put(key, createTime);
                    proposalLock.writeLock().unlock();
                    SyncProposal newSyncProposal =
                        JsonUtils.fromJson(event.getData().getData(), SyncProposal.class);
                    this.listener.onCreate(key, newSyncProposal);
                    break;
                  case NODE_UPDATED:
                    proposalLock.writeLock().lock();
                    latestProposalTimes.remove(key);
                    proposalLock.writeLock().unlock();
                    SyncProposal afterSyncProposal =
                        JsonUtils.fromJson(event.getData().getData(), SyncProposal.class);
                    SyncProposal beforeSyncProposal =
                        JsonUtils.fromJson(event.getOldData().getData(), SyncProposal.class);
                    this.listener.onUpdate(key, beforeSyncProposal, afterSyncProposal);
                    break;
                  case NODE_REMOVED:
                    LOGGER.error("node_remove should not happened!");
                    break;
                  default:
                    LOGGER.error("unknown event should not happened!");
                    break;
                }
              } catch (Exception e) {
                LOGGER.info("encounter exception when process event: ", e);
              }
            });
    try {
      this.proposalCache.start();
    } catch (Exception e) {
      LOGGER.error("register proposal listener for {} failure: ", category, e);
      throw new NetworkException("register proposal listener failure.", e);
    }
  }

  private void registerGlobalVoteListener() throws NetworkException {
    this.voteCache =
        new TreeCache(client, String.format(VOTE_PROPOSAL_CONTAINER_TEMPLATE, category));
    this.voteCache
        .getListenable()
        .addListener(
            (curatorFramework, event) -> {
              if (event.getData() == null
                  || event.getData().getPath() == null
                  || !event.getData().getPath().contains("voter")) {
                return;
              }
              String key = event.getData().getPath().split("/")[3];
              switch (event.getType()) {
                case NODE_ADDED:
                  SyncVote vote = JsonUtils.fromJson(event.getData().getData(), SyncVote.class);
                  proposalLock.readLock().lock();
                  VoteListener voteLister = voteListeners.get(key);
                  proposalLock.readLock().unlock();
                  if (voteLister != null) {
                    voteLister.receive(key, vote);
                  }
                  break;
                case NODE_UPDATED:
                  LOGGER.error("node_update should not happened!");
                  break;
                case NODE_REMOVED:
                  LOGGER.error("node_remove should not happened!");
                  break;
                default:
                  LOGGER.error("unknown event should not happened!");
                  break;
              }
            });
    try {
      this.voteCache.start();
    } catch (Exception e) {
      LOGGER.error("register global vote listener for {} failure: ", category, e);
      throw new NetworkException("register global vote listener failure.", e);
    }
  }

  @Override
  public boolean startProposal(String key, SyncProposal syncProposal, VoteListener listener)
      throws NetworkException {
    long createTime = System.currentTimeMillis();
    String lockPath = String.format(PROTOCOL_PROPOSAL_LOCK_TEMPLATE, this.category, key);
    InterProcessMutex mutex = new InterProcessMutex(this.client, lockPath);
    boolean release = false;
    try {
      if (!mutex.acquire(100, TimeUnit.MILLISECONDS)) {
        LOGGER.info("acquire lock for {} failure, another process hold the lock", lockPath);
        return false;
      }
      LOGGER.info("acquire lock for {} success", lockPath);
      release = true;
      // 判断有没有刚创建的 proposal
      if (this.client
              .checkExists()
              .forPath(String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key))
          != null) {
        List<String> children =
            this.client
                .getChildren()
                .forPath(String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key));
        if (!children.isEmpty()) {
          long lastCreateTime = Long.parseLong(children.get(children.size() - 1).split("_")[1]);
          if (lastCreateTime + MAX_NETWORK_LATENCY > createTime) {
            LOGGER.warn(
                "start protocol for " + category + "-" + key + " failure, due to repeated request");
            return false;
          }
        }
      }
      // 创建票箱
      this.client
          .create()
          .creatingParentsIfNeeded()
          .forPath(
              String.format(VOTE_PROPOSAL_TEMPLATE, this.category, key)
                  + PATH_SEPARATOR
                  + "proposal_"
                  + createTime);
      LOGGER.info("create vote container success");
      proposalLock.writeLock().lock();
      latestProposalTimes.put(key, createTime);
      voteListeners.put(key, listener);
      proposalLock.writeLock().unlock();

      // 再注册 proposal
      syncProposal.setCreateTime(createTime);
      this.client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(
              String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key)
                  + PATH_SEPARATOR
                  + "proposal_"
                  + createTime,
              JsonUtils.toJson(syncProposal));
      LOGGER.info("create protocol success");
      return true;
    } catch (Exception e) {
      LOGGER.error("start protocol for {}-{} failure: ", category, key, e);
      throw new NetworkException("start protocol failure: ", e);
    } finally {
      if (release) {
        try {
          mutex.release();
        } catch (Exception e) {
          LOGGER.error("get error when release interprocess lock for {}", e);
        }
      }
    }
  }

  @Override
  public void registerProposalListener(ProposalListener listener) {
    this.listener = listener;
  }

  @Override
  public void voteFor(String key, SyncVote vote) throws NetworkException, VoteExpiredException {
    LOGGER.info("vote for {} from {}", key, vote.getVoter());
    long voter = vote.getVoter();
    try {
      long createTime = 0L;
      proposalLock.readLock().lock();
      createTime = latestProposalTimes.getOrDefault(key, 0L);
      proposalLock.readLock().unlock();
      if (createTime == 0) {
        throw new VoteExpiredException("vote for expired proposal: " + key);
      }
      this.client
          .create()
          .withMode(CreateMode.PERSISTENT)
          .forPath(
              String.format(VOTE_PROPOSAL_TEMPLATE, this.category, key)
                  + PATH_SEPARATOR
                  + "proposal_"
                  + createTime
                  + PATH_SEPARATOR
                  + "voter_"
                  + voter,
              JsonUtils.toJson(vote));
    } catch (VoteExpiredException e) {
      LOGGER.error("encounter execute error in vote: ", e);
      throw e;
    } catch (Exception e) {
      LOGGER.error("vote for {}-{} failure: ", category, key, e);
      throw new NetworkException("vote failure: ", e);
    }
  }

  @Override
  public void endProposal(String key, SyncProposal syncProposal)
      throws NetworkException, ExecutionException {
    long updateTime = System.currentTimeMillis();
    String lockPath = String.format(PROTOCOL_PROPOSAL_LOCK_TEMPLATE, this.category, key);
    InterProcessMutex mutex = new InterProcessMutex(this.client, lockPath);

    boolean release = false;
    try {
      mutex.acquire();
      release = true;
      List<String> children =
          this.client
              .getChildren()
              .forPath(String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key));
      if (children.isEmpty()) {
        throw new ExecutionException("can't find proposal for " + key);
      }
      long createTime = Long.parseLong(children.get(children.size() - 1).split("_")[1]);
      syncProposal.setCreateTime(createTime);
      syncProposal.setUpdateTime(updateTime);
      this.client
          .setData()
          .forPath(
              String.format(PROTOCOL_PROPOSAL_TEMPLATE, this.category, key)
                  + PATH_SEPARATOR
                  + "proposal_"
                  + createTime,
              JsonUtils.toJson(syncProposal));

      proposalLock.writeLock().lock();
      latestProposalTimes.remove(key);
      voteListeners.remove(key).end(key);
      proposalLock.writeLock().unlock();
    } catch (ExecutionException e) {
      LOGGER.error("encounter execution exception when end proposal for {}: ", key, e);
      throw e;
    } catch (Exception e) {
      LOGGER.error("end protocol for {}-{} failure: ", category, key, e);
      throw new NetworkException("end protocol failure: ", e);
    } finally {
      if (release) {
        try {
          mutex.release();
        } catch (Exception e) {
          LOGGER.error("get error when release interprocess lock for {}", lockPath, e);
        }
      }
    }
  }

  @Override
  public String toString() {
    return "ZooKeeperProtocolImpl{" + "category='" + category + '\'' + '}';
  }

  public void close() {
    this.proposalCache.close();
  }
}
