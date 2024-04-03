package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;
import cn.edu.tsinghua.iginx.conf.FilePermissionConfig;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;

import static org.junit.Assert.*;

public class FilePermissionManagerTest {

  private static final String PERMISSION_CONFIG_FILE = "conf/file-permission-test.properties";

  @Test
  public void testInit() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);
  }

  @Test
  public void testCheckNull() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);
    try {
      Predicate<Path> predicate = manager.getChecker(null, null, FileAccessType.READ);
      predicate.test(Paths.get("test"));
      fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }

    try {
      Predicate<Path> predicate = manager.getChecker(null, Module.DEFAULT, null);
      predicate.test(Paths.get("test"));
      fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }

    Predicate<Path> predicate = manager.getChecker(null, Module.DEFAULT, FileAccessType.READ);
    try {
      predicate.test(null);
      fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }
  }

  @Test
  public void testCheckFallback() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    Predicate<Path> predicate = manager.getChecker(null, Module.UDF, FileAccessType.EXECUTE);
    assertTrue(predicate.test(Paths.get("udf.py")));
    assertFalse(predicate.test(Paths.get("udf.sh")));
  }

  @Test
  public void testCheckDefaultModule() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    Predicate<Path> predicate = manager.getChecker(null, Module.UDF, FileAccessType.EXECUTE);
    assertTrue(predicate.test(Paths.get("test.bat")));
  }

  @Test
  public void testCheckDefaultUser() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    Predicate<Path> predicate = manager.getChecker("root", Module.UDF, FileAccessType.EXECUTE);
    assertTrue(predicate.test(Paths.get("test.sh")));

    Predicate<Path> rootPredicate = manager.getChecker("test", Module.UDF, FileAccessType.EXECUTE);
    assertFalse(rootPredicate.test(Paths.get("test.sh")));
  }

  @Test
  public void testCheckDefault() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    Predicate<Path> predicate = manager.getChecker(null, Module.UDF, FileAccessType.READ);
    assertTrue(predicate.test(Paths.get("test.py")));
  }

  @Test
  public void testCheckEmpty() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    Predicate<Path> predicate = manager.getChecker(null, Module.UDF);
    assertTrue(predicate.test(Paths.get("test.sh")));
  }

  @Test
  public void testCheckMultiple() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    Predicate<Path> predicate = manager.getChecker(null, Module.UDF, FileAccessType.EXECUTE, FileAccessType.WRITE);
    assertFalse(predicate.test(Paths.get("test.py")));
  }
}
