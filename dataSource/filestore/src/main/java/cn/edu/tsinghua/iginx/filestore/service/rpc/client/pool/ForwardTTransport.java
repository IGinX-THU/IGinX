package cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ForwardTTransport extends TTransport {

  private final TTransport transport;

  public ForwardTTransport(TTransport transport) {
    this.transport = Objects.requireNonNull(transport);
  }

  @Override
  public boolean isOpen() {
    return this.transport.isOpen();
  }

  @Override
  public boolean peek() {
    return this.transport.peek();
  }

  @Override
  public void open() throws TTransportException {
    this.transport.open();
  }

  @Override
  public void close() {
    this.transport.close();
  }

  @Override
  public int read(ByteBuffer dst) throws TTransportException {
    return this.transport.read(dst);
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    return this.transport.read(buf, off, len);
  }

  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    return this.transport.readAll(buf, off, len);
  }

  @Override
  public void write(byte[] buf) throws TTransportException {
    this.transport.write(buf);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    this.transport.write(buf, off, len);
  }

  @Override
  public int write(ByteBuffer src) throws TTransportException {
    return this.transport.write(src);
  }

  @Override
  public void flush() throws TTransportException {
    this.transport.flush();
  }

  @Override
  public byte[] getBuffer() {
    return this.transport.getBuffer();
  }

  @Override
  public int getBufferPosition() {
    return this.transport.getBufferPosition();
  }

  @Override
  public int getBytesRemainingInBuffer() {
    return this.transport.getBytesRemainingInBuffer();
  }

  @Override
  public void consumeBuffer(int len) {
    this.transport.consumeBuffer(len);
  }

  @Override
  public TConfiguration getConfiguration() {
    return this.transport.getConfiguration();
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {
    this.transport.updateKnownMessageSize(size);
  }

  @Override
  public void checkReadBytesAvailable(long numBytes) throws TTransportException {
    this.transport.checkReadBytesAvailable(numBytes);
  }
}
