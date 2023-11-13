package org.apache.hadoop.fs;

public interface PathFilter {
  boolean accept(Path path);
}
