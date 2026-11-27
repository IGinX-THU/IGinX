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

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/** Orders synchronous writes, initial snapshots and delayed watch notifications. */
final class UserChangeTracker {
  private final Map<String, Change> applied = new HashMap<>();
  private final BiConsumer<String, UserMeta> consumer;

  UserChangeTracker(BiConsumer<String, UserMeta> consumer) {
    this.consumer = consumer;
  }

  synchronized void accept(String username, UserMeta user, long revision) {
    Change previous = applied.get(username);
    // ZooKeeper's NODE_REMOVED carries the deleted node's last mzxid. A delete
    // therefore wins a tie. A recreated node has a newer zxid. Keep tombstones
    // so a delayed add cannot resurrect an already deleted account.
    if (previous != null
        && (revision < previous.revision
            || (revision == previous.revision && (previous.deleted || user != null)))) {
      return;
    }
    consumer.accept(username, user);
    applied.put(username, new Change(revision, user == null));
  }

  private static final class Change {
    final long revision;
    final boolean deleted;

    Change(long revision, boolean deleted) {
      this.revision = revision;
      this.deleted = deleted;
    }
  }
}
