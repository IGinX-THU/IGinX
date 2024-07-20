package cn.edu.tsinghua.iginx.filestore.service.rpc;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.ClientConfig;
import com.google.common.collect.Range;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class RpcConfig extends AbstractConfig {

  ClientConfig client;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateSubConfig(problems, Fields.client, client);
    return problems;
  }
}
