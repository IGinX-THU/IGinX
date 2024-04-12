package cn.edu.tsinghua.iginx.auth;

import static cn.edu.tsinghua.iginx.auth.utils.FilePermissionRuleNameFilters.defaultRules;
import static cn.edu.tsinghua.iginx.auth.utils.FilePermissionRuleNameFilters.udfRulesWithDefault;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.conf.FilePermissionConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

public class FilePermissionManagerTest {

  private static final String PERMISSION_CONFIG_FILE = "conf/file-permission-test.properties";
  private static final String PERMISSION_CONFIG_CHINESE_FILE =
      "conf/file-permission-test-chinese.properties";

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
      FilePermissionManager.Checker predicate = manager.getChecker(null, null, FileAccessType.READ);
      predicate.test(Paths.get("test"));
      fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }

    try {
      FilePermissionManager.Checker predicate = manager.getChecker(null, defaultRules(), null);
      predicate.test(Paths.get("test"));
      fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }

    FilePermissionManager.Checker predicate =
        manager.getChecker(null, defaultRules(), FileAccessType.READ);
    try {
      predicate.test((Path) null);
      fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }
  }

  @Test
  public void testCheckFallback() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate =
        manager.getChecker(null, udfRulesWithDefault(), FileAccessType.EXECUTE);
    assertTrue(predicate.test(Paths.get("udf.py")));
    assertFalse(predicate.test(Paths.get("udf.sh")));
  }

  @Test
  public void testCheckDefaultModule() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate =
        manager.getChecker(null, udfRulesWithDefault(), FileAccessType.EXECUTE);
    assertTrue(predicate.test(Paths.get("test.bat")));
  }

  @Test
  public void testCheckDefaultUser() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate =
        manager.getChecker("root", udfRulesWithDefault(), FileAccessType.EXECUTE);
    assertTrue(predicate.test(Paths.get("test.sh")));

    FilePermissionManager.Checker rootPredicate =
        manager.getChecker("test", udfRulesWithDefault(), FileAccessType.EXECUTE);
    assertFalse(rootPredicate.test(Paths.get("test.sh")));
  }

  @Test
  public void testCheckDefault() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate =
        manager.getChecker(null, udfRulesWithDefault(), FileAccessType.READ);
    assertTrue(predicate.test(Paths.get("test.py")));
  }

  @Test
  public void testCheckEmpty() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate = manager.getChecker(null, udfRulesWithDefault());
    assertTrue(predicate.test(Paths.get("test.sh")));
  }

  @Test
  public void testCheckMultiple() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate =
        manager.getChecker(
            null, udfRulesWithDefault(), FileAccessType.EXECUTE, FileAccessType.WRITE);
    assertFalse(predicate.test(Paths.get("test.py")));
  }

  @Test
  public void testCheckChinese() throws ConfigurationException {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_CHINESE_FILE);
    config.reload();
    FilePermissionManager manager = new FilePermissionManager(config);

    FilePermissionManager.Checker predicate =
        manager.getChecker(
            "测试用户",
            ruleName -> ruleName.equals("第一条规则"),
            FileAccessType.EXECUTE,
            FileAccessType.WRITE);
    assertFalse(predicate.test(Paths.get("不允许.py")));
  }
}
