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
