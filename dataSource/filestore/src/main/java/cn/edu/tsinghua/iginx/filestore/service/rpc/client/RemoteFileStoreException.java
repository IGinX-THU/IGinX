package cn.edu.tsinghua.iginx.filestore.service.rpc.client;

import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.thrift.RpcException;

public class RemoteFileStoreException extends FileStoreException {
  public RemoteFileStoreException(String message, Throwable e) {
    super(message, e);
  }
}
