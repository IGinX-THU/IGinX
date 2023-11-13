package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.OutputStream;

public class CompressionOutputStream extends OutputStream {
  @Override
  public void write(int b) throws IOException {
    throw new IOException("unimplemented!");
  }
}
