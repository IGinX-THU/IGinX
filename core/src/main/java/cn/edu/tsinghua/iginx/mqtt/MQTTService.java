package cn.edu.tsinghua.iginx.mqtt;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import com.google.common.collect.Lists;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTService implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTService.class);

  private static MQTTService INSTANCE;

  private final Server server = new Server();

  protected MQTTService() {}

  public static MQTTService getInstance() {
    if (INSTANCE == null) {
      synchronized (MQTTService.class) {
        if (INSTANCE == null) {
          INSTANCE = new MQTTService();
        }
      }
    }
    return INSTANCE;
  }

  public void start() {
    Config iginxConfig = ConfigDescriptor.getInstance().getConfig();
    IConfig config = createBrokerConfig(iginxConfig);
    List<InterceptHandler> handlers = Lists.newArrayList(new PublishHandler(iginxConfig));
    IAuthenticator authenticator = new BrokerAuthenticator();

    server.startServer(config, handlers, null, authenticator, null);

    LOGGER.info(
        "Start MQTT service successfully, listening on ip {} port {}",
        iginxConfig.getMqttHost(),
        iginxConfig.getMqttPort());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOGGER.info("Stopping IoTDB MQTT service...");
                  shutdown();
                  LOGGER.info("IoTDB MQTT service stopped.");
                }));
  }

  private IConfig createBrokerConfig(Config iginxConfig) {
    Properties properties = new Properties();
    properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, iginxConfig.getMqttHost());
    properties.setProperty(
        BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(iginxConfig.getMqttPort()));
    properties.setProperty(
        BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE,
        String.valueOf(iginxConfig.getMqttHandlerPoolSize()));
    properties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
    properties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
    properties.setProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
    properties.setProperty(
        BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
        String.valueOf(iginxConfig.getMqttMaxMessageSize()));
    return new MemoryConfig(properties);
  }

  @Override
  public void run() {
    start();
  }

  public void shutdown() {
    server.stopServer();
  }
}
