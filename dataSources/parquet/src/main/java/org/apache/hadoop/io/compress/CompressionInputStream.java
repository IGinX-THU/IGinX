package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;

public class CompressionInputStream extends InputStream {
  @Override
  public int read() throws IOException {
    throw new IOException("unimplemented!");
  }
}
