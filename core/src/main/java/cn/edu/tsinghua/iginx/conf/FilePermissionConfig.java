package cn.edu.tsinghua.iginx.conf;

import cn.edu.tsinghua.iginx.conf.entity.FilePermissionDescriptor;
import cn.edu.tsinghua.iginx.conf.parser.FilePermissionsParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.configuration2.reloading.ReloadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePermissionConfig implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilePermissionConfig.class);

  private static final FilePermissionConfig INSTANCE =
      new FilePermissionConfig("file-permission.properties");

  static {
    try {
      INSTANCE.reload();
    } catch (Throwable e) {
      LOGGER.error("Failed to load permission configuration from", e);
    }
  }

  public static FilePermissionConfig getInstance() {
    return INSTANCE;
  }

  private static final String RELOAD_INTERVAL_KEY = "refreshInterval";
  private static final long DEFAULT_RELOAD_INTERVAL = 5 * 1000;
  private static final String DEFAULT_CHARSET = "UTF-8";

  private final ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> builder;

  public FilePermissionConfig(String filename) {
    builder =
        new ReloadingFileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
            .configure(
                new Parameters()
                    .properties()
                    .setReloadingRefreshDelay(0L)
                    .setEncoding(DEFAULT_CHARSET)
                    .setFileName(filename));

    builder
        .getReloadingController()
        .addEventListener(
            ReloadingEvent.ANY,
            event -> {
              try {
                reload();
              } catch (ConfigurationException e) {
                LOGGER.error("Failed to reload permission configuration file", e);
              }
            });
  }

  private final List<Runnable> hooks = new ArrayList<>();

  public synchronized void onReload(Runnable runnable) {
    hooks.add(runnable);
  }

  public synchronized void reload() throws ConfigurationException {
    LOGGER.info("Reloading permission configuration file");

    ImmutableConfiguration config = getConfiguration();
    updateReloadTrigger(config.getLong(RELOAD_INTERVAL_KEY, DEFAULT_RELOAD_INTERVAL));

    updateFilePermissions(config);
    hooks.forEach(Runnable::run);
  }

  synchronized ImmutableConfiguration getConfiguration() throws ConfigurationException {
    builder.getReloadingController().checkForReloading(null);
    builder.resetResult();
    return builder.getConfiguration();
  }

  private PeriodicReloadingTrigger trigger;

  public synchronized void close() {
    if (trigger != null) {
      trigger.shutdown();
    }
  }

  private Long reloadInterval;

  Long getReloadInterval() {
    return reloadInterval;
  }

  private void updateReloadTrigger(Long newReloadInterval) {
    if (newReloadInterval.equals(reloadInterval)) {
      return;
    }
    reloadInterval = newReloadInterval;
    close();
    trigger =
        new PeriodicReloadingTrigger(
            builder.getReloadingController(), null, reloadInterval, TimeUnit.MILLISECONDS);
    trigger.start();
  }

  private final AtomicReference<List<FilePermissionDescriptor>> filePermissions =
      new AtomicReference<>(Collections.emptyList());

  private void updateFilePermissions(ImmutableConfiguration config) {
    List<FilePermissionDescriptor> newFilePermissions = FilePermissionsParser.parseList(config);
    filePermissions.set(Collections.unmodifiableList(newFilePermissions));
  }

  public List<FilePermissionDescriptor> getFilePermissions() {
    return filePermissions.get();
  }
}
