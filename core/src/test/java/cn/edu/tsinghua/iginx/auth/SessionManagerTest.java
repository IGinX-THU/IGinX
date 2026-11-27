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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class SessionManagerTest {
  private UserMeta user(String name, String password) {
    return new UserMeta(
        name, password, UserType.OrdinaryUser, EnumSet.of(AuthType.Read, AuthType.Write));
  }

  @Test
  public void ids() {
    UserMeta user = user("u", "password");
    SessionManager manager = new SessionManager(name -> user, true);
    Set<Long> ids = new HashSet<>();
    int[] ones = new int[64];
    int permissions = (1 << AuthType.Read.getValue()) | (1 << AuthType.Write.getValue());
    long previous = 0;
    int ascending = 0, descending = 0;
    for (int i = 0; i < 100000; i++) {
      long id = manager.openSession("u", "password");
      assertNotEquals(0, id);
      assertTrue(ids.add(id));
      assertEquals(permissions, id & 15);
      if (i > 0) {
        if (Long.compareUnsigned(id >>> 4, previous >>> 4) > 0) ascending++;
        else descending++;
      }
      previous = id;
      for (int bit = 4; bit < 64; bit++) if ((id & (1L << bit)) != 0) ones[bit]++;
    }
    for (int bit = 4; bit < 64; bit++) assertTrue(ones[bit] > 49000 && ones[bit] < 51000);
    assertTrue(ascending > 40000 && descending > 40000);
    long forged = new java.security.SecureRandom().nextLong();
    while (ids.contains(forged)) forged = new java.security.SecureRandom().nextLong();
    assertFalse(manager.checkSession(forged, AuthType.Read));
    assertFalse(manager.checkSession(0, AuthType.Read));
    long closed = ids.iterator().next();
    manager.closeSession(closed);
    assertFalse(manager.checkSession(closed, AuthType.Read));
    assertEquals(0, manager.openSession("u", "wrong"));
  }

  @Test
  public void closeByUserAndVersion() {
    Map<String, UserMeta> users = new ConcurrentHashMap<>();
    users.put("u", user("u", "old"));
    users.put("v", user("v", "old"));
    SessionManager manager = new SessionManager(users::get, true);
    long[] ids = {manager.openSession("u"), manager.openSession("u"), manager.openSession("u")};
    long other = manager.openSession("v");
    users.remove("u");
    manager.invalidateUser("u", null);
    for (long id : ids) assertFalse(manager.checkSession(id, AuthType.Read));
    manager.invalidateUser("v", users.get("v").copy());
    assertTrue(manager.checkSession(other, AuthType.Read));
    users.get("v").setPassword("new");
    assertFalse(manager.checkSession(other, AuthType.Read));
    long current = manager.openSession("v", "new");
    users.get("v").setAuths(EnumSet.of(AuthType.Read));
    manager.invalidateUser("v", users.get("v"));
    assertFalse(manager.checkSession(current, AuthType.Write));
    assertTrue(manager.getSessionIds().isEmpty());
  }

  @Test
  public void notificationKeepsSessionsOpenedWithCurrentVersion() {
    Map<String, UserMeta> users = new ConcurrentHashMap<>();
    users.put("u", user("u", "old"));
    SessionManager manager = new SessionManager(users::get, true);
    long old = manager.openSession("u", "old");
    users.put("u", user("u", "new"));
    long current = manager.openSession("u", "new");
    manager.invalidateUser("u", users.get("u"));
    assertFalse(manager.isValidSession(old));
    assertTrue(manager.isValidSession(current));
  }

  @Test
  public void concurrentOpenAndRevoke() throws Exception {
    Map<String, UserMeta> users = new ConcurrentHashMap<>();
    users.put("u", user("u", "0"));
    users.put("v", user("v", "unchanged"));
    SessionManager manager = new SessionManager(users::get, true);
    long other = manager.openSession("v", "unchanged");
    ExecutorService pool = Executors.newFixedThreadPool(8);
    AtomicBoolean done = new AtomicBoolean();
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < 8; i++)
        futures.add(
            pool.submit(
                () -> {
                  while (!done.get()) {
                    long id = manager.openSession("u");
                    manager.checkSession(id, AuthType.Read);
                    manager.closeSession(id);
                  }
                }));
      for (int i = 1; i <= 200; i++) {
        UserMeta user = user("u", Integer.toString(i));
        users.put("u", user);
        manager.invalidateUser("u", user);
      }
    } finally {
      done.set(true);
      pool.shutdown();
    }
    for (Future<?> future : futures) future.get(10, TimeUnit.SECONDS);
    users.put("u", user("u", "final"));
    manager.invalidateUser("u", users.get("u"));
    for (long id : manager.getSessionIds())
      if (id != other) assertFalse(manager.isValidSession(id));
    assertTrue(manager.isValidSession(other));
    assertEquals(java.util.Collections.singleton(other), manager.getSessionIds());
  }

  @Test
  public void legacySessionSurvivesUserChange() {
    cn.edu.tsinghua.iginx.utils.SnowFlakeUtils.init(0);
    UserMeta user = user("u", "old");
    SessionManager manager = new SessionManager(name -> user, false);
    long id = manager.openSession("u");
    user.setPassword("new");
    assertTrue(manager.checkSession(id, AuthType.Read));
  }

  @org.junit.Test
  public void recreatedAccountInvalidatesSessionEvenWhenPasswordAndPermissionsMatch() {
    java.util.Map<String, UserMeta> users = new java.util.HashMap<>();
    UserMeta original = user("u", "password");
    original.setIncarnation(10);
    users.put("u", original);
    SessionManager manager = new SessionManager(users::get, true);
    long session = manager.openSession("u", "password");
    UserMeta noOpUpdate = original.copy();
    noOpUpdate.setRevision(20);
    users.put("u", noOpUpdate);
    manager.invalidateUser("u", noOpUpdate);
    org.junit.Assert.assertTrue(manager.isValidSession(session));
    UserMeta recreated = original.copy();
    recreated.setIncarnation(30);
    users.put("u", recreated);
    org.junit.Assert.assertFalse(manager.isValidSession(session));
  }
}
