package cn.edu.tsinghua.iginx.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import java.util.HashMap;
import java.util.Map;

public class JsonUtils {

  public static byte[] toJson(Object o) {
    String tmp = JSON.toJSONString(o, Feature.WriteClassName);
    return tmp.getBytes();
  }

  public static <T> T fromJson(byte[] data, Class<T> clazz) {
    return JSON.parseObject(data, clazz);
  }

  public static Map<String, String> transformToSS(String content) {
    if (content == null || content.equals("{}")) {
      return null;
    }
    Map<String, Object> rawMap = JSON.parseObject(content);
    Map<String, String> ret = new HashMap<>();
    rawMap.forEach((key, value) -> ret.put(key, (String) value));
    return ret;
  }

  public static Map<String, Integer> transform(String content) {
    Map<String, Object> rawMap = JSON.parseObject(content);
    Map<String, Integer> ret = new HashMap<>();
    rawMap.forEach((key, value) -> ret.put(key, (Integer) value));
    return ret;
  }
}
