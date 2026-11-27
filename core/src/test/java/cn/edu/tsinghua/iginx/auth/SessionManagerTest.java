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
  public void legacySessionSurvivesUserChange() {
    cn.edu.tsinghua.iginx.utils.SnowFlakeUtils.init(0);
    UserMeta user = user("u", "old");
    SessionManager manager = new SessionManager(name -> user, false);
    long id = manager.openSession("u");
    user.setPassword("new");
    assertTrue(manager.checkSession(id, AuthType.Read));
  }
}
