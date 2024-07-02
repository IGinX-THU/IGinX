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
package cn.edu.tsinghua.iginx.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializeUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializeUtils.class);

  public static <T extends Serializable> byte[] serialize(T obj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
      os.writeObject(obj);
    } catch (IOException e) {
      LOGGER.error("encounter error when serialize: ", e);
    }
    return bos.toByteArray();
  }

  public static <T extends Serializable> T deserialize(byte[] data, Class<T> clazz) {
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    Object obj = null;
    try (ObjectInputStream in = new ObjectInputStream(bin)) {
      obj = in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.error("encounter error when deserialize: ", e);
    }
    if (obj == null) return null;
    return clazz.cast(obj);
  }
}
