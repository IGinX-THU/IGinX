package cn.edu.tsinghua.iginx.utils;

import com.alibaba.fastjson2.JSON;
import java.util.HashMap;
import java.util.Map;

public class JsonUtils {

    public static final String TYPENAME = "iginxtype";

    public static byte[] toJson(Object o) {
        return JSON.toJSONBytes(o);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        return JSON.parseObject(data, clazz);
    }

    public static byte[] addType(String type, String typeSpecificName, byte[] data) {
        return addType(type, typeSpecificName, data, 0);
    }

    public static byte[] addType(String type, String typeSpecificName, byte[] data, int begIndex) {
        StringBuilder json = new StringBuilder(new String(data));
        int index = json.indexOf(type, begIndex);
        if (index != -1) {
            // 3 is the length of the ":{", and the position +1
            int addIndex = json.indexOf("{", index);
            json.insert(addIndex + 1, "\"" + TYPENAME + "\":" + "\"" + typeSpecificName + "\",");
        }
        return json.toString().getBytes();
    }

    public static String mapToJson(Map<String, String> map) {
        return JSON.toJSONString(map);
    }

    public static Map<String, Integer> transform(String content) {
        Map<String, Object> rawMap = JSON.parseObject(content);
        Map<String, Integer> ret = new HashMap<>();
        rawMap.forEach((key, value) -> ret.put(key, (Integer) value));
        return ret;
    }

    public static Map<String, String> transformToSS(String content) {
        if (content == null || content.equals("{}")) return null;
        Map<String, Object> rawMap = JSON.parseObject(content);
        Map<String, String> ret = new HashMap<>();
        rawMap.forEach((key, value) -> ret.put(key, (String) value));
        return ret;
    }
}
