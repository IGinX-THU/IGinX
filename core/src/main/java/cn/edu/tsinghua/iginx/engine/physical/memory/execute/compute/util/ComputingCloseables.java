package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

public class ComputingCloseables {

  private ComputingCloseables() {
  }

  public static void close(Iterable<? extends ComputingCloseable> closeables) throws ComputeException {
    for (ComputingCloseable closeable : closeables) {
      closeable.close();
    }
  }

}
