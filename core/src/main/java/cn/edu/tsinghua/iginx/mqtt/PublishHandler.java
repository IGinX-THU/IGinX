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

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PublishHandler extends AbstractInterceptHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PublishHandler.class);

  private final IginxWorker worker = IginxWorker.getInstance();

  private final IPayloadFormatter payloadFormat;

  private final long sessionId;

  public PublishHandler(Config config) {
    payloadFormat =
        PayloadFormatManager.getInstance().getFormatter(config.getMqttPayloadFormatter());
    // open session as root user
    sessionId = SessionManager.getInstance().openSession(config.getUsername());
  }

  @Override
  public String getID() {
    return "iginx-mqtt-broker-listener";
  }

  @Override
  public void onPublish(InterceptPublishMessage msg) {
    String clientId = msg.getClientID();
    ByteBuf payload = msg.getPayload();
    String topic = msg.getTopicName();
    String username = msg.getUsername();
    MqttQoS qos = msg.getQos();

    LOGGER.debug(
        "Receive publish message. clientId: {}, username: {}, qos = {}, topic: {}, payload: {}",
        clientId,
        username,
        qos,
        topic,
        payload);

    List<Message> events = payloadFormat.format(payload);
    if (events == null) {
      return;
    }

    // 重排序数据，并过滤空事件
    events =
        events.stream()
            .filter(Objects::nonNull)
            .sorted(
                (o1, o2) -> {
                  if (o1.getKey() != o2.getKey()) {
                    return Long.compare(o1.getKey(), o2.getKey());
                  }
                  return o1.getPath().compareTo(o2.getPath());
                })
            .collect(Collectors.toList());
    if (events.size() == 0) {
      return;
    }

    // 计算实际写入的数据
    List<String> paths =
        events.stream().map(Message::getPath).distinct().sorted().collect(Collectors.toList());
    Map<String, DataType> dataTypeMap = new HashMap<>();
    for (Message message : events) {
      if (dataTypeMap.containsKey(message.getPath())) {
        if (dataTypeMap.get(message.getPath()) != message.getDataType()) {
          LOGGER.error(
              "meet error when process message, data type conflict: {} with type {} and {}",
              message.getPath(),
              dataTypeMap.get(message.getPath()),
              message.getDataType());
          return;
        }
      } else {
        dataTypeMap.put(message.getPath(), message.getDataType());
      }
    }
    List<DataType> dataTypeList = new ArrayList<>();
    for (String path : paths) {
      dataTypeList.add(dataTypeMap.get(path));
    }

    List<Long> timestamps = new ArrayList<>();
    List<ByteBuffer> bitmapList = new ArrayList<>();
    List<ByteBuffer> valuesList = new ArrayList<>();
    int from = 0, to = 0;
    while (from < events.size()) {
      long timestamp = events.get(from).getKey();
      while (to < events.size() && events.get(to).getKey() == timestamp) {
        to++;
      }
      timestamps.add(timestamp);
      Bitmap bitmap = new Bitmap(paths.size());
      Object[] values = new Object[paths.size()];
      for (int i = 0; i < paths.size(); i++) {
        Message event = events.get(from);
        if (event.getPath().equals(paths.get(i))) { // 序列正好匹配上
          bitmap.mark(i);
          values[i] = event.getValue();
          from++;
        } else {
          values[i] = null;
        }
      }
      bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
      valuesList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
    }

    // 采用行接口写入数据
    InsertNonAlignedRowRecordsReq req = new InsertNonAlignedRowRecordsReq();
    req.setSessionId(sessionId);
    req.setKeys(ByteUtils.getColumnByteBuffer(timestamps.toArray(), DataType.LONG));
    req.setPaths(paths);
    req.setDataTypeList(dataTypeList);
    req.setValuesList(valuesList);
    req.setBitmapList(bitmapList);

    Status status = worker.insertNonAlignedRowRecords(req);
    LOGGER.debug("event process result: {}", status);
    msg.getPayload().release();
  }
}
