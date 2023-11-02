package cn.edu.tsinghua.iginx.parquet.local;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.parquet.AbstractExecutorTest;
import cn.edu.tsinghua.iginx.parquet.exec.NewExecutor;
import cn.edu.tsinghua.iginx.parquet.tools.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExecutorTest extends AbstractExecutorTest {

  private static final Logger logger = LoggerFactory.getLogger(LocalExecutorTest.class);

  protected static String dataDir = "./src/test/resources/dataDir";

  protected static String dummyDir = "./src/test/resources/dummyDir";

  public LocalExecutorTest() {
    createOrClearDir(Paths.get(dataDir));
    createOrClearDir(Paths.get(dummyDir));
    try {
      executor =
          new NewExecutor(
              false,
              false,
              Paths.get(dataDir).toAbsolutePath().toString(),
              Paths.get(dummyDir).toAbsolutePath().toString());
    } catch (StorageInitializationException e) {
      logger.error(String.format("Can't get parquet local executor: %s", e.getMessage()));
    }
  }

  public boolean createOrClearDir(Path path) {
    if (Files.exists(path)) {
      File duDir = new File(path.toString());
      FileUtils.deleteFile(duDir);
    }
    if (!createDir(path)) {
      logger.error("Can't create dir: " + path);
      return false;
    }

    return true;
  }

  public boolean createDir(Path path) {
    try {
      if (!Files.exists(path)) {
        Files.createDirectories(path);
      }
      return true;
    } catch (IOException e) {
      logger.error("Can't create dir: " + path);
      return false;
    }
  }

  @Override
  public String newDU() {
    try {
      DUIndexLock.writeLock().lock();
      String unitName = "unit" + String.format("%08d", DU_INDEX);
      Path path = Paths.get(dataDir, unitName);
      if (!Files.exists(path)) {
        Files.createDirectories(path);
      }
      DU_INDEX++;
      return unitName;
    } catch (Exception e) {
      logger.error("initializing new du index failed: " + e.getMessage());
    } finally {
      DUIndexLock.writeLock().unlock();
    }
    return "";
  }
}
