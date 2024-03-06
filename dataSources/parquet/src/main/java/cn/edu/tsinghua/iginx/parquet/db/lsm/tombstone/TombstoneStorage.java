package cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ObjectFormat;
import cn.edu.tsinghua.iginx.parquet.shared.CachePool;
import cn.edu.tsinghua.iginx.parquet.shared.Constants;
import cn.edu.tsinghua.iginx.parquet.shared.Shared;
import cn.edu.tsinghua.iginx.parquet.shared.exception.StorageRuntimeException;
import java.io.*;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TombstoneStorage<K extends Comparable<K>, F> implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneStorage.class);
  private final Shared shared;
  private final Path dir;
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final ObjectFormat<K> keyFormat;
  private final ObjectFormat<F> fieldFormat;

  public TombstoneStorage(
      Shared shared, Path dir, ObjectFormat<K> keyFormat, ObjectFormat<F> fieldFormat) {
    this.shared = shared;
    this.dir = dir;
    this.keyFormat = keyFormat;
    this.fieldFormat = fieldFormat;
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
  public Tombstone<K, F> get(String tableName) {
    String fileName = getFileName(tableName);
    lock.readLock().lock();
    try {
      CachePool.Cacheable cacheable =
          shared.getCachePool().asMap().computeIfAbsent(fileName, this::loadCache);
      if (!(cacheable instanceof CachedTombstone)) {
        throw new StorageRuntimeException("unexpected cacheable type: " + cacheable.getClass());
      }
      return ((CachedTombstone<K, F>) cacheable).getTombstone();
    } finally {
      lock.readLock().unlock();
    }
  }

  @SuppressWarnings("unchecked")
  public void delete(Set<String> tables, Consumer<Tombstone<K, F>> action) {
    lock.readLock().lock();
    try {
      for (String tableName : tables) {
        String fileName = getFileName(tableName);
        shared
            .getCachePool()
            .asMap()
            .compute(
                fileName,
                (k, v) -> {
                  Tombstone<K, F> tombstone;
                  if (v == null) {
                    tombstone = new Tombstone<>();
                  } else {
                    if (!(v instanceof CachedTombstone)) {
                      throw new StorageRuntimeException(
                          "unexpected cacheable type: " + v.getClass());
                    }
                    tombstone = ((CachedTombstone<K, F>) v).getTombstone().copy();
                  }
                  action.accept(tombstone);
                  return flushCache(k, tombstone);
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

  private CachedTombstone<K, F> flushCache(String fileName, Tombstone<K, F> tombstone) {
    String tempFileName = fileName + Constants.SUFFIX_FILE_TEMP;
    Path path = Paths.get(fileName);
    Path tempPath = Paths.get(tempFileName);

    String json = SerializeUtils.serialize(tombstone, keyFormat, fieldFormat);

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
    return new CachedTombstone<>(tombstone, json.length() + fileName.length());
  }

  private CachedTombstone<K, F> loadCache(String fileName) {
    try (FileReader fr = new FileReader(Paths.get(fileName).toFile());
        BufferedReader br = new BufferedReader(fr)) {
      String json = br.lines().collect(Collectors.joining());
      Tombstone<K, F> tombstone =
          SerializeUtils.deserializeRangeTombstone(json, keyFormat, fieldFormat);
      return new CachedTombstone<>(tombstone, json.length() + fileName.length());
    } catch (FileNotFoundException e) {
      return new CachedTombstone<>(fileName.length());
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public void close() {
    // do nothing
  }

  public static class CachedTombstone<K extends Comparable<K>, F> implements CachePool.Cacheable {

    private final Tombstone<K, F> tombstone;

    private final int weight;

    public CachedTombstone(Tombstone<K, F> tombstone, int size) {
      this.tombstone = tombstone;
      this.weight = size + 32;
    }

    public CachedTombstone(int length) {
      this(null, length);
    }

    public Tombstone<K, F> getTombstone() {
      return tombstone == null ? new Tombstone<>() : tombstone;
    }

    @Override
    public int getWeight() {
      return weight;
    }
  }
}
