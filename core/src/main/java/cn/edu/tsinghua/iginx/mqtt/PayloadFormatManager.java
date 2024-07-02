package cn.edu.tsinghua.iginx.mqtt;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PayloadFormatManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PayloadFormatManager.class);

  private static final PayloadFormatManager instance = new PayloadFormatManager();

  private final Map<String, IPayloadFormatter> formatters;

  private PayloadFormatManager() {
    this.formatters = new HashMap<>();
  }

  public static PayloadFormatManager getInstance() {
    return instance;
  }

  public IPayloadFormatter getFormatter(String formatterClassName) {
    IPayloadFormatter formatter;
    synchronized (formatters) {
      formatter = formatters.get(formatterClassName);
      if (formatter == null) {
        try {
          Class<? extends IPayloadFormatter> clazz =
              this.getClass()
                  .getClassLoader()
                  .loadClass(formatterClassName)
                  .asSubclass(IPayloadFormatter.class);
          formatter = clazz.getConstructor().newInstance();
          formatters.put(formatterClassName, formatter);
        } catch (ClassNotFoundException
            | InstantiationException
            | IllegalAccessException
            | NoSuchMethodException
            | InvocationTargetException e) {
          LOGGER.error("Failed to load formatter: {}", formatterClassName, e);
        }
      }
    }
    return formatter;
  }
}
