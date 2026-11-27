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
package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

  private static volatile SessionManager active;
  private final Function<String, UserMeta> users;
  private final boolean secure;
  private final SecureRandom random = new SecureRandom();
  private final Map<Long, SessionIdentity> identities = new ConcurrentHashMap<>();
  private final Set<Long> sessionIds = ConcurrentHashMap.newKeySet();

  private SessionManager(UserManager userManager) {
    this(userManager::getUser, SecurityConfig.getInstance().isEnabled());
    active = this;
  }

  SessionManager(Function<String, UserMeta> users, boolean secure) {
    this.users = users;
    this.secure = secure;
  }

  private static final class InstanceHolder {
    static final SessionManager instance = new SessionManager(UserManager.getInstance());
  }

  public static SessionManager getInstance() {
    return InstanceHolder.instance;
  }

  public Set<Long> getSessionIds() {
    return secure ? new HashSet<>(sessionIds) : sessionIds;
  }

  public boolean checkSession(long sessionId, AuthType auth) {
    if (!isValidSession(sessionId)) {
      return false;
    }
    return ((1L << auth.getValue()) & sessionId) != 0;
  }

  public long openSession(String username) {
    UserMeta userMeta = users.apply(username);
    if (userMeta == null) {
      throw new IllegalArgumentException("non-existed user: " + username);
    }
    return createSession(userMeta.copy());
  }

  public synchronized long openSession(String username, String password) {
    UserMeta user = users.apply(username);
    if (user == null) return 0;
    UserMeta snapshot = user.copy();
    if (!snapshot.getPassword().equals(password)) return 0;
    long id = createSession(snapshot);
    return isValidSession(id) ? id : 0;
  }

  private synchronized long createSession(UserMeta userMeta) {
    String username = userMeta.getUsername();
    long sessionId;
    if (secure) {
      do {
        sessionId = random.nextLong() & ~15L;
      } while (sessionId == 0 || identities.containsKey(sessionId | permissions(userMeta)));
    } else {
      sessionId =
          (username.hashCode() + System.currentTimeMillis() + SnowFlakeUtils.getInstance().nextId())
              << 4;
    }
    for (AuthType auth : userMeta.getAuths()) {
      sessionId += (1L << auth.getValue());
    }
    if (secure) identities.put(sessionId, new SessionIdentity(username, version(userMeta)));
    else LOGGER.info("new session id comes: {}", sessionId);
    sessionIds.add(sessionId);
    return sessionId;
  }

  public synchronized void closeSession(long sessionId) {
    if (!secure) LOGGER.info("session id {} is removed.", sessionId);
    sessionIds.remove(sessionId);
    identities.remove(sessionId);
    if (secure) {
      cn.edu.tsinghua.iginx.resource.QueryResourceManager.getInstance().releaseSession(sessionId);
      UploadRegistry.getInstance().releaseSession(sessionId);
    }
  }

  public boolean isSessionClosed(long sessionId) {
    return !isValidSession(sessionId);
  }

  public synchronized boolean isValidSession(long id) {
    if (!sessionIds.contains(id)) return false;
    if (!secure) return true;
    SessionIdentity identity = identities.get(id);
    UserMeta current = identity == null ? null : users.apply(identity.username);
    if (current == null || !Arrays.equals(identity.version, version(current.copy()))) {
      closeSession(id);
      return false;
    }
    return true;
  }

  public String getUsername(long id) {
    if (!isValidSession(id)) return null;
    SessionIdentity identity = identities.get(id);
    return identity == null ? null : identity.username;
  }

  public synchronized void invalidateUser(String username, UserMeta current) {
    byte[] expected = current == null ? null : version(current.copy());
    identities.forEach(
        (id, identity) -> {
          if (identity.username.equals(username) && !Arrays.equals(identity.version, expected))
            closeSession(id);
        });
  }

  public static void userChanged(String username, UserMeta user) {
    SessionManager manager = active;
    if (manager != null && manager.secure) manager.invalidateUser(username, user);
  }

  private static int permissions(UserMeta user) {
    int mask = 0;
    for (AuthType auth : user.getAuths()) mask |= 1 << auth.getValue();
    return mask;
  }

  private static byte[] version(UserMeta user) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(user.getPassword().getBytes(StandardCharsets.UTF_8));
      digest.update((byte) permissions(user));
      digest.update(
          java.nio.ByteBuffer.allocate(Long.BYTES).putLong(user.getIncarnation()).array());
      return digest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final class SessionIdentity {
    private final String username;
    private final byte[] version;

    private SessionIdentity(String username, byte[] version) {
      this.username = username;
      this.version = version;
    }
  }
}
