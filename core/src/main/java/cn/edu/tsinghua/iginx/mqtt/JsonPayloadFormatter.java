package cn.edu.tsinghua.iginx.mqtt;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPayloadFormatter implements IPayloadFormatter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonPayloadFormatter.class);

  private static final String JSON_KEY_PATH = "path";
  private static final String JSON_KEY_TIMESTAMP = "timestamp";
  private static final String JSON_KEY_DATATYPE = "dataType";
  private static final String JSON_KEY_VALUE = "value";

  public JsonPayloadFormatter() {
    LOGGER.info("use JsonPayloadFormatter as mqtt message formatter.");
  }

  @Override
  public List<Message> format(ByteBuf payload) {
    if (payload == null) {
      return null;
    }
    String txt = payload.toString(StandardCharsets.UTF_8);
    LOGGER.info("receive message: {}", txt);
    JSONArray jsonArray = JSON.parseArray(txt);
    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = jsonArray.getJSONObject(i);
      String path = jsonObject.getString(JSON_KEY_PATH);
      long timestamp = jsonObject.getLong(JSON_KEY_TIMESTAMP);
      DataType dataType = null;
      Object value = null;
      switch (jsonObject.getString(JSON_KEY_DATATYPE)) {
        case "int":
          dataType = DataType.INTEGER;
          value = jsonObject.getInteger(JSON_KEY_VALUE);
          break;
        case "long":
          dataType = DataType.LONG;
          value = jsonObject.getLong(JSON_KEY_VALUE);
          break;
        case "boolean":
          dataType = DataType.BOOLEAN;
          value = jsonObject.getBoolean(JSON_KEY_VALUE);
          break;
        case "float":
          dataType = DataType.FLOAT;
          value = jsonObject.getFloat(JSON_KEY_VALUE);
          break;
        case "double":
          dataType = DataType.DOUBLE;
          value = jsonObject.getDouble(JSON_KEY_VALUE);
          break;
        case "text":
          dataType = DataType.BINARY;
          value = jsonObject.getString(JSON_KEY_VALUE).getBytes(StandardCharsets.UTF_8);
          break;
      }
      if (value != null) {
        Message message = new Message();
        message.setPath(path);
        message.setDataType(dataType);
        message.setKey(timestamp);
        message.setValue(value);
        messages.add(message);
      }
    }
    return messages;
  }
}
