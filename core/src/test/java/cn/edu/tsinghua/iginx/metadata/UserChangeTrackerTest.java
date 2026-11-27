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
package cn.edu.tsinghua.iginx.metadata;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import java.util.*;
import org.junit.Test;

public class UserChangeTrackerTest {
  @Test
  public void delayedAddsAndSnapshotsCannotUndoDeletionOrPasswordChange() {
    Map<String, UserMeta> cache = new HashMap<>();
    List<String> changes = new ArrayList<>();
    UserChangeTracker tracker =
        new UserChangeTracker(
            (name, user) -> {
              if (user == null) cache.remove(name);
              else cache.put(name, user);
              changes.add(user == null ? "deleted" : user.getPassword());
            });
    UserMeta old = user("old"), changed = user("new");
    tracker.accept("u", old, 10);
    tracker.accept("u", changed, 20);
    tracker.accept("u", old, 10); // delayed watch / initial load snapshot
    assertEquals("new", cache.get("u").getPassword());
    tracker.accept("u", null, 20); // ZooKeeper removal shares the last mzxid
    tracker.accept("u", changed, 20);
    tracker.accept("u", old, 10);
    assertFalse(cache.containsKey("u"));
    tracker.accept("u", old, 30); // recreation has a new creation zxid
    tracker.accept("u", null, 20); // previous incarnation's delayed removal
    assertEquals("old", cache.get("u").getPassword());
    assertEquals(Arrays.asList("old", "new", "deleted", "old"), changes);
  }

  @Test
  public void etcdDeleteRevisionAndDuplicateNotificationsAreOrdered() {
    List<UserMeta> changes = new ArrayList<>();
    UserChangeTracker tracker = new UserChangeTracker((name, user) -> changes.add(user));
    tracker.accept("u", user("one"), 10);
    tracker.accept("u", null, 11);
    tracker.accept("u", user("one"), 10);
    tracker.accept("u", null, 11);
    tracker.accept("u", user("two"), 12);
    assertEquals(3, changes.size());
    assertNull(changes.get(1));
    assertEquals("two", changes.get(2).getPassword());
  }

  private static UserMeta user(String password) {
    return new UserMeta("u", password, UserType.OrdinaryUser, EnumSet.of(AuthType.Read));
  }
}
