package cn.edu.tsinghua.iginx.parquet.db.lsm.tombstone;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ObjectFormat;
import cn.edu.tsinghua.iginx.parquet.utils.Constants;
import cn.edu.tsinghua.iginx.parquet.utils.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.utils.exception.StorageRuntimeException;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class TombstoneStorage<K extends Comparable<K>, F> implements Closeable {
  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TombstoneStorage.class);
  private final Path dir;
  private final Map<String, Tombstone<K, F>> tombstones = new HashMap<>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final ObjectFormat<K> keyFormat;
  private final ObjectFormat<F> fieldFormat;

  public TombstoneStorage(Path dir, ObjectFormat<K> keyFormat, ObjectFormat<F> fieldFormat) {
    this.dir = dir;
    this.keyFormat = keyFormat;
    this.fieldFormat = fieldFormat;
    reload();
  }

  private void reload() {
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, "*" + Constants.SUFFIX_FILE_TOMBSTONE)) {
      for (Path path : stream) {
        String fileName = path.getFileName().toString();
        String tableName =
            fileName.substring(0, fileName.length() - Constants.SUFFIX_FILE_TOMBSTONE.length());
        Tombstone<K, F> tombstone = load(path);
        tombstones.put(tableName, tombstone);
      }
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public Tombstone<K, F> get(String tableName) {
    lock.readLock().lock();
    try {
      return tombstones.get(tableName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(Set<String> tables, Consumer<Tombstone<K, F>> action) {
    lock.writeLock().lock();
    try {
      for (String table : tables) {
        Tombstone<K, F> tombstone = tombstones.get(table);
        if (tombstone == null) {
          throw new NotIntegrityException("table " + table + " not exists");
        }
        action.accept(tombstone);
        flush(table, tombstone);
      }
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      MoreFiles.deleteDirectoryContents(dir, RecursiveDeleteOption.ALLOW_INSECURE);
      Files.deleteIfExists(dir);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void flush(String table, Tombstone<K, F> tombstone) throws IOException {
    String fileName = table + Constants.SUFFIX_FILE_TOMBSTONE;
    String tempFileName = fileName + Constants.SUFFIX_FILE_TEMP;
    Path path = dir.resolve(fileName);
    Path tempPath = dir.resolve(tempFileName);

    String json = SerializeUtils.serialize(tombstone, keyFormat, fieldFormat);

    LOGGER.debug("flush tombstone to temp file: {}", tempPath);
    try (FileWriter fw = new FileWriter(tempPath.toFile());
        BufferedWriter bw = new BufferedWriter(fw)) {
      bw.write(json);
    }
    LOGGER.debug("rename temp file to file: {}", path);
    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
  }

  private Tombstone<K, F> load(Path path) throws IOException {
    try (FileReader fr = new FileReader(path.toFile());
        BufferedReader br = new BufferedReader(fr)) {
      String json = br.lines().collect(Collectors.joining());
      return SerializeUtils.deserializeRangeTombstone(json, keyFormat, fieldFormat);
    }
  }

  public void close() {
    // do nothing
  }
}
