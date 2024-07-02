package cn.edu.tsinghua.iginx.session_v2;

public interface IginXClient extends AutoCloseable {

  WriteClient getWriteClient();

  AsyncWriteClient getAsyncWriteClient();

  QueryClient getQueryClient();

  DeleteClient getDeleteClient();

  UsersClient getUserClient();

  ClusterClient getClusterClient();

  TransformClient getTransformClient();

  void close();
}
