package cn.edu.tsinghua.iginx.conf;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FilePermissionConfigTest {

  private static final String PERMISSION_CONFIG_FILE = "conf/file-permission-test.properties";
  private static final String PERMISSION_CONFIG_FILE_CHINESE =
      "conf/file-permission-test-chinese.properties";
  private static final String PERMISSION_CONFIG_TEMP = "target/permission-test-temp.properties";

  @Before
  public void setup() throws IOException {
    try (InputStream stream =
        FilePermissionConfig.class.getClassLoader().getResourceAsStream(PERMISSION_CONFIG_FILE)) {
      assert stream != null;
      Files.copy(stream, Paths.get(PERMISSION_CONFIG_TEMP));
    }
  }

  @After
  public void cleanup() throws IOException {
    Files.deleteIfExists(Paths.get(PERMISSION_CONFIG_TEMP));
  }

  @Test
  public void testInit() {
    FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE);
    config.close();
  }

  @Test(expected = ConfigurationException.class)
  public void testNoSuchFile() throws ConfigurationException {
    try (FilePermissionConfig config =
        new FilePermissionConfig(PERMISSION_CONFIG_FILE + "-no-such-file")) {
      config.reload();
    }
  }

  @Test
  public void testLoad() throws ConfigurationException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE)) {
      ImmutableConfiguration configuration = config.getConfiguration();
      List<String> keys = new ArrayList<>();
      configuration.getKeys().forEachRemaining(keys::add);
      assertEquals(
          Arrays.asList(
              "refreshInterval",
              "default.udfRule1.include",
              "default.udfRule1.execute",
              "default.udfRule2.include",
              "default.udfRule2.execute",
              "default.defaultFirstRule.include",
              "default.defaultFirstRule.execute",
              "default.defaultSecondRule.include",
              "default.defaultSecondRule.execute",
              "default.defaultSecondRule.write",
              "root.defaultRule.include",
              "root.defaultRule.read",
              "root.defaultRule.write",
              "root.defaultRule.execute"),
          keys);

      assertEquals(100, configuration.getInt("refreshInterval"));
      assertEquals("glob:**.py", configuration.getString("default.udfRule1.include"));
      assertEquals(true, configuration.getBoolean("default.udfRule1.execute"));
      assertEquals("glob:**.{sh,py}", configuration.getString("default.udfRule2.include"));
      assertEquals(false, configuration.getBoolean("default.udfRule2.execute"));
      assertEquals("glob:**.{sh,bat}", configuration.getString("default.defaultFirstRule.include"));
      assertEquals(true, configuration.getBoolean("default.defaultFirstRule.execute"));
      assertEquals("glob:**", configuration.getString("default.defaultSecondRule.include"));
      assertEquals(false, configuration.getBoolean("default.defaultSecondRule.execute"));
      assertEquals(false, configuration.getBoolean("default.defaultSecondRule.write"));
      assertEquals("glob:**", configuration.getString("root.defaultRule.include"));
      assertEquals(true, configuration.getBoolean("root.defaultRule.read"));
      assertEquals(true, configuration.getBoolean("root.defaultRule.write"));
      assertEquals(true, configuration.getBoolean("root.defaultRule.execute"));
    }
  }

  @Test
  public void testDefaultInterval() throws ConfigurationException, IOException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_TEMP)) {
      assertNull(config.getReloadInterval());
      config.reload();
      Assert.assertEquals(100, config.getReloadInterval().longValue());
      try (RandomAccessFile file = new RandomAccessFile(PERMISSION_CONFIG_TEMP, "rw")) {
        file.write("#".getBytes());
      }
      config.reload();
      Assert.assertEquals(5000, config.getReloadInterval().longValue());
    }
  }

  @Test
  public void testReload() throws ConfigurationException, IOException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_TEMP)) {
      assertNull(config.getReloadInterval());
      config.reload();
      Assert.assertEquals(100, config.getReloadInterval().longValue());
      try (RandomAccessFile file = new RandomAccessFile(PERMISSION_CONFIG_TEMP, "rw")) {
        file.seek("refreshInterval=".length());
        file.write("200".getBytes());
      }
      config.reload();
      Assert.assertEquals(200, config.getReloadInterval().longValue());
    }
  }

  @Test
  public void testOnReload() throws ConfigurationException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE)) {
      boolean[] called = {false, false, false};
      config.onReload(
          () -> {
            called[0] = true;
          });
      config.onReload(
          () -> {
            called[1] = true;
          });
      config.onReload(
          () -> {
            called[2] = true;
          });
      config.reload();
      assertArrayEquals(new boolean[] {true, true, true}, called);
    }
  }

  @Test
  public void testReloadInterval()
      throws ConfigurationException, IOException, InterruptedException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_TEMP)) {
      assertNull(config.getReloadInterval());
      config.reload();
      Assert.assertEquals(100, config.getReloadInterval().longValue());
      Path temp = Files.createTempFile("permission-test-temp", ".properties");
      Files.copy(Paths.get(PERMISSION_CONFIG_TEMP), temp, StandardCopyOption.REPLACE_EXISTING);
      try (RandomAccessFile file = new RandomAccessFile(temp.toString(), "rw")) {
        file.seek("refreshInterval=".length());
        file.write("010".getBytes());
      }
      Files.move(temp, Paths.get(PERMISSION_CONFIG_TEMP), StandardCopyOption.REPLACE_EXISTING);
      Assert.assertEquals(100, config.getReloadInterval().longValue());
      Thread.sleep(200);
      Assert.assertEquals(10, config.getReloadInterval().longValue());
    }
  }

  @Test
  public void testDuplicateInterval()
      throws ConfigurationException, IOException, InterruptedException {
    try (RandomAccessFile file = new RandomAccessFile(PERMISSION_CONFIG_TEMP, "rw")) {
      file.seek(file.length());
      file.write(System.lineSeparator().getBytes());
      file.write("refreshInterval=200".getBytes());
    }
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_TEMP)) {
      assertNull(config.getReloadInterval());
      config.reload();
      Assert.assertEquals(100, config.getReloadInterval().longValue());
    }
  }

  @Test
  public void testFilePermission() throws ConfigurationException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE)) {
      config.reload();
      List<FilePermissionDescriptor> permissions = config.getFilePermissions();
      LinkedHashSet<FilePermissionDescriptor> resultSet = new LinkedHashSet<>(permissions);

      Set<FilePermissionDescriptor> expect =
          new HashSet<>(
              Arrays.asList(
                  new FilePermissionDescriptor(
                      null,
                      "udfRule1",
                      "glob:**.py",
                      new HashMap<FileAccessType, Boolean>() {
                        {
                          put(FileAccessType.EXECUTE, true);
                        }
                      }),
                  new FilePermissionDescriptor(
                      null,
                      "udfRule2",
                      "glob:**.{sh,py}",
                      new HashMap<FileAccessType, Boolean>() {
                        {
                          put(FileAccessType.EXECUTE, false);
                        }
                      }),
                  new FilePermissionDescriptor(
                      null,
                      "defaultFirstRule",
                      "glob:**.{sh,bat}",
                      new HashMap<FileAccessType, Boolean>() {
                        {
                          put(FileAccessType.EXECUTE, true);
                        }
                      }),
                  new FilePermissionDescriptor(
                      null,
                      "defaultSecondRule",
                      "glob:**",
                      new HashMap<FileAccessType, Boolean>() {
                        {
                          put(FileAccessType.EXECUTE, false);
                          put(FileAccessType.WRITE, false);
                        }
                      }),
                  new FilePermissionDescriptor(
                      "root",
                      "defaultRule",
                      "glob:**",
                      new HashMap<FileAccessType, Boolean>() {
                        {
                          put(FileAccessType.READ, true);
                          put(FileAccessType.WRITE, true);
                          put(FileAccessType.EXECUTE, true);
                        }
                      })));

      assertEquals(expect, resultSet);

      Set<String> order = new HashSet<>();
      for (FilePermissionDescriptor descriptor : permissions) {
        order.add(descriptor.getRuleName());
        if (descriptor.getPattern().equals("udfRule2")) {
          if (!order.contains("udfRule1")) {
            fail("The order of the permissions is wrong");
          }
        }
      }
    }
  }

  @Test
  public void testOnReloadFilePermission() throws ConfigurationException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE)) {
      List<FilePermissionDescriptor> permissions = config.getFilePermissions();
      config.reload();
      List<FilePermissionDescriptor> loadedPermissions = config.getFilePermissions();
      assertNotEquals(permissions, loadedPermissions);
    }
  }

  @Test
  public void testChinese() throws ConfigurationException {
    try (FilePermissionConfig config = new FilePermissionConfig(PERMISSION_CONFIG_FILE_CHINESE)) {
      config.reload();
      List<FilePermissionDescriptor> permissions = config.getFilePermissions();
      assertEquals(
          Collections.singletonList(
              new FilePermissionDescriptor(
                  "测试用户",
                  "第一条规则",
                  "glob:**/不允许.py",
                  new HashMap<FileAccessType, Boolean>() {
                    {
                      put(FileAccessType.EXECUTE, false);
                    }
                  })),
          permissions);
    }
  }
}
