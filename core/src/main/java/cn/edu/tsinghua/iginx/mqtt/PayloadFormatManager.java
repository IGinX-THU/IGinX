/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
