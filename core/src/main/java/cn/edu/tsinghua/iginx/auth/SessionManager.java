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
package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

  private final UserManager userManager;
  private final Set<Long> sessionIds = ConcurrentHashMap.newKeySet();

  private SessionManager(UserManager userManager) {
    this.userManager = userManager;
  }

  private static final class InstanceHolder {
    static final SessionManager instance = new SessionManager(UserManager.getInstance());
  }

  public static SessionManager getInstance() {
    return InstanceHolder.instance;
  }

  public Set<Long> getSessionIds() {
    return sessionIds;
  }

  public boolean checkSession(long sessionId, AuthType auth) {
    if (!sessionIds.contains(sessionId)) {
      return false;
    }
    return ((1L << auth.getValue()) & sessionId) != 0;
  }

  public long openSession(String username) {
    UserMeta userMeta = userManager.getUser(username);
    if (userMeta == null) {
      throw new IllegalArgumentException("non-existed user: " + username);
    }
    long sessionId =
        (username.hashCode() + System.currentTimeMillis() + SnowFlakeUtils.getInstance().nextId())
            << 4;
    for (AuthType auth : userMeta.getAuths()) {
      sessionId += (1L << auth.getValue());
    }
    LOGGER.info("new session id comes: {}", sessionId);
    sessionIds.add(sessionId);
    return sessionId;
  }

  public void closeSession(long sessionId) {
    LOGGER.info("session id {} is removed.", sessionId);
    sessionIds.remove(sessionId);
  }

  public boolean isSessionClosed(long sessionId) {
    return !sessionIds.contains(sessionId);
  }
}
