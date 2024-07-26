package cn.edu.tsinghua.iginx.filestore.service.remote;

import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.ClientConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.RemoteService;
import cn.edu.tsinghua.iginx.filestore.service.rpc.server.Server;
import cn.edu.tsinghua.iginx.filestore.service.storage.AbstractStorageServiceTest;
import com.typesafe.config.Config;

import java.net.InetSocketAddress;
import java.util.Random;

public class AbstractRemoteServiceTest extends AbstractStorageServiceTest {

  private final InetSocketAddress address;

  protected AbstractRemoteServiceTest(String type, Config config) {
    super(type, config);
    this.address = new InetSocketAddress("localhost", new Random().nextInt(50000) + 15530);
  }

  private Service service;
  private Server server;

  @Override
  protected Service getService() throws Exception {
    if(service == null) {
      server = new Server(address,super.getService());
      service = new RemoteService(address, new ClientConfig());
    }
    return service;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if(service!=null){
      service.close();
      server.close();
      service = null;
      server = null;
    }
  }
}
