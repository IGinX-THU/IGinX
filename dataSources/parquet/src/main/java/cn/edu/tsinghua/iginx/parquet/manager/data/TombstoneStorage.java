/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.util.CachePool;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.Shared;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import java.io.*;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TombstoneStorage implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneStorage.class);
  private final Shared shared;
  private final Path dir;
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  public TombstoneStorage(Shared shared, Path dir) {
    this.shared = shared;
    this.dir = dir;
    cleanTempFiles();
  }

  private void cleanTempFiles() {
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, path -> path.endsWith(Constants.SUFFIX_FILE_TEMP))) {
      for (Path path : stream) {
        LOGGER.info("remove temp file {}", path);
        Files.deleteIfExists(path);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("no dir named {}", dir);
    } catch (IOException e) {
      LOGGER.error("failed to clean temp files", e);
    }
  }

  @SuppressWarnings("unchecked")
  public AreaSet<Long, String> get(String tableName) {
    String fileName = getFileName(tableName);
    lock.readLock().lock();
    try {
      CachePool.Cacheable cacheable =
          shared.getCachePool().asMap().computeIfAbsent(fileName, this::loadCache);
      if (!(cacheable instanceof CachedTombstone)) {
        throw new StorageRuntimeException("unexpected cacheable type: " + cacheable.getClass());
      }
      return ((CachedTombstone<Long, String>) cacheable).getTombstone();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeTable(String name) {
    lock.writeLock().lock();
    try {
      String fileName = getFileName(name);
      shared.getCachePool().asMap().remove(fileName);
      Files.deleteIfExists(Paths.get(fileName));
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  public void delete(Set<String> tables, Consumer<AreaSet<Long, String>> action) {
    lock.readLock().lock();
    try {
      for (String tableName : tables) {
        String fileName = getFileName(tableName);
        shared
            .getCachePool()
            .asMap()
            .compute(
                fileName,
                (Long, v) -> {
                  AreaSet<Long, String> areas = new AreaSet<>();
                  if (v != null) {
                    if (!(v instanceof CachedTombstone)) {
                      throw new StorageRuntimeException(
                          "unexpected cacheable type: " + v.getClass());
                    }
                    areas.addAll(((CachedTombstone<Long, String>) v).getTombstone());
                  }
                  action.accept(areas);
                  return flushCache(Long, areas);
                });
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      try (DirectoryStream<Path> stream =
          Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_TOMBSTONE)) {
        for (Path path : stream) {
          Files.deleteIfExists(path);
          String fileName = path.toString();
          shared.getCachePool().asMap().remove(fileName);
        }
      }
      Files.deleteIfExists(dir);
    } catch (NoSuchFileException e) {
      LOGGER.trace("Not a directory to clear: {}", dir);
    } catch (DirectoryNotEmptyException e) {
      LOGGER.warn("directory not empty to clear: {}", dir);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private String getFileName(String tableName) {
    return dir.resolve(tableName + Constants.SUFFIX_FILE_TOMBSTONE).toString();
  }

  private CachedTombstone<Long, String> flushCache(String fileName, AreaSet<Long, String> areas) {
    String tempFileName = fileName + Constants.SUFFIX_FILE_TEMP;
    Path path = Paths.get(fileName);
    Path tempPath = Paths.get(tempFileName);

    String json = SerializeUtils.serialize(areas, new LongFormat(), new StringFormat());

    LOGGER.debug("flush tombstone to temp file: {}", tempPath);
    try {
      Files.createDirectories(dir);
      try (FileWriter fw = new FileWriter(tempPath.toFile());
          BufferedWriter bw = new BufferedWriter(fw)) {
        bw.write(json);
      }
      LOGGER.debug("rename temp file to file: {}", path);
      Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
    return new CachedTombstone<>(areas, json.length() + fileName.length());
  }

  private CachedTombstone<Long, String> loadCache(String fileName) {
    try (FileReader fr = new FileReader(Paths.get(fileName).toFile());
        BufferedReader br = new BufferedReader(fr)) {
      String json = br.lines().collect(Collectors.joining());
      AreaSet<Long, String> areas =
          SerializeUtils.deserializeRangeTombstone(json, new LongFormat(), new StringFormat());
      return new CachedTombstone<>(areas, json.length() + fileName.length());
    } catch (FileNotFoundException e) {
      return new CachedTombstone<>(fileName.length());
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public void close() {
    // do nothing
  }

  public static class CachedTombstone<Long extends Comparable<Long>, String>
      implements CachePool.Cacheable {

    private final AreaSet<Long, String> areas;

    private final int weight;

    public CachedTombstone(AreaSet<Long, String> areas, int size) {
      this.areas = areas;
      this.weight = size + 32;
    }

    public CachedTombstone(int length) {
      this(null, length);
    }

    public AreaSet<Long, String> getTombstone() {
      return areas == null ? new AreaSet<>() : areas;
    }

    @Override
    public int getWeight() {
      return weight;
    }
  }
}
