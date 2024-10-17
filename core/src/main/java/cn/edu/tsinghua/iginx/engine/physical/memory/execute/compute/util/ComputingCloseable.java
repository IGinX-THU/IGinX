package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

public interface ComputingCloseable extends AutoCloseable {

    @Override
    void close() throws ComputeException;
}
