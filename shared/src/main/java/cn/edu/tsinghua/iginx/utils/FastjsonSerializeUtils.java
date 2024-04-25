package cn.edu.tsinghua.iginx.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;

public class FastjsonSerializeUtils {

  public static <T> String serialize(T obj) {
    if (obj == null) {
      throw new NullPointerException();
    }
    return JSON.toJSONString(obj, JSONWriter.Feature.WriteClassName, JSONWriter.Feature.FieldBased);
  }

  public static <T> T deserialize(String json, Class<T> clazz) {
    return JSON.parseObject(json, clazz, JSONReader.Feature.SupportAutoType);
  }
}
