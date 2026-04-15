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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.tombstone;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.AtomFlushPathWrapper;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TombstoneStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneStorage.class);
  private final CachePool cachePool;

  public TombstoneStorage(CachePool cachePool) {
    this.cachePool = cachePool;
  }

  public @Nullable Tombstone get(Path path) {
    return (Tombstone) cachePool.asMap().computeIfAbsent(path, p -> loadCache(path));
  }

  public void delete(Path path, Tombstone tombstone) {
    cachePool
        .asMap()
        .compute(
            path,
            (key, value) -> {
              Tombstone newValue;
              if (value != null) {
                newValue = Tombstone.merge((Tombstone) value, tombstone);
              } else {
                newValue = tombstone;
              }
              flushCache(path, newValue);
              return newValue;
            });
  }

  private void flushCache(Path path, Tombstone tombstone) {
    try (AtomFlushPathWrapper wrapper = new AtomFlushPathWrapper(path, true)) {
      MoreFiles.createParentDirectories(wrapper.getTmpPath());
      try (ObjectOutputStream oos =
          new ObjectOutputStream(
              Files.newOutputStream(
                  wrapper.getTmpPath(),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.TRUNCATE_EXISTING))) {
        oos.writeObject(tombstone);
      }
      wrapper.commit();
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  private @Nullable Tombstone loadCache(Path path) {
    if (!Files.exists(path)) {
      return null;
    }
    try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) {
      return (Tombstone) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new StorageRuntimeException(e);
    }
  }
}
