package cn.edu.tsinghua.iginx.mqtt;

import io.netty.buffer.ByteBuf;
import java.util.List;

public interface IPayloadFormatter {

  List<Message> format(ByteBuf payload);
}
