package cn.edu.tsinghua.iginx.filestore.service;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.RemoteService;
import cn.edu.tsinghua.iginx.filestore.service.rpc.server.Server;
import cn.edu.tsinghua.iginx.filestore.service.storage.StorageService;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;

public class FileStoreService implements Service {

  private final static Logger LOGGER = LoggerFactory.getLogger(FileStoreService.class);

  private final Service service;
  private final Server server;

  public FileStoreService(InetSocketAddress address, FileStoreConfig config) throws TTransportException, FileStoreException {
    if (config.isServer()) {
      this.service = new StorageService(config.getData(), config.getDummy());
      LOGGER.info("Starting file store server in {}", address);
      this.server = new Server(address, this.service);
      this.server.serve();
    } else {
      LOGGER.info("Will connect to file store server at {}", address);
      this.service = new RemoteService(address, config.getClient());
      this.server = null;
    }
  }

  @Override
  public Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileStoreException {
    return service.getUnits(prefix);
  }

  @Override
  public RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate) throws FileStoreException {
    return service.query(unit, target, aggregate);
  }

  @Override
  public void delete(DataUnit unit, DataTarget target) throws FileStoreException {
    service.delete(unit, target);
  }

  @Override
  public void insert(DataUnit unit, DataView dataView) throws FileStoreException {
    service.insert(unit, dataView);
  }

  @Override
  public void close() throws FileStoreException {
    if (server != null) {
      server.stop();
    }
    service.close();
  }
}
