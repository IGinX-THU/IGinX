package cn.edu.tsinghua.iginx.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import java.util.Map;

public class JsonUtils {

  public static byte[] toJson(Object o) {
    return JSON.toJSONBytes(o);
  }

  public static <T> T fromJson(byte[] data, Class<T> clazz) {
    return JSON.parseObject(data, clazz);
  }

  public static <K, V> Map<K, V> parseMap(String data, Class<K> keyType, Class<V> valueType) {
    return JSON.parseObject(data, new TypeReference<Map<K, V>>(keyType, valueType) {});
  }
}
