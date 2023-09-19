package cn.edu.tsinghua.iginx.parquet.local;

import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.parquet.AbstractExecutorTest;
import cn.edu.tsinghua.iginx.parquet.exec.NewExecutor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExecutorTest extends AbstractExecutorTest {

  private static final Logger logger = LoggerFactory.getLogger(LocalExecutorTest.class);

  private static final String DRIVER_NAME = "org.duckdb.DuckDBDriver";

  private static final String CONN_URL = "jdbc:duckdb:";

  public LocalExecutorTest() {
    try {
      executor = new NewExecutor(getConnection(), false, false, dataDir, dummyDir);
    } catch (StorageInitializationException e) {
      logger.error(String.format("Can't get parquet local executor: %s", e.getMessage()));
      e.printStackTrace();
    }
  }

  private Connection getConnection() {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      logger.error(String.format("Class %s not found", DRIVER_NAME));
    }

    Connection connection = null;
    try {
      connection = DriverManager.getConnection(CONN_URL);
    } catch (SQLException e) {
      logger.error("cannot get local duckdb connection");
    }
    return connection;
  }

  @Override
  public String newDU() {
    try {
      DUIndexLock.writeLock().lock();
      String unitName = "unit" + String.format("%08d", DU_INDEX);
      Path path = Paths.get(dataDir, unitName);
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
      DU_INDEX++;
      return unitName;
    } catch (Exception e) {
      logger.error("initializing new du index failed: " + e.getMessage());
      e.printStackTrace();
    } finally {
      DUIndexLock.writeLock().unlock();
    }
    return "";
  }
}
