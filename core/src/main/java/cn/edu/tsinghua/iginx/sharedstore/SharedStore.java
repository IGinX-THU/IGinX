package cn.edu.tsinghua.iginx.sharedstore;

public interface SharedStore {

    boolean put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    boolean delete(byte[] key);

    boolean exists(byte[] key);

}
