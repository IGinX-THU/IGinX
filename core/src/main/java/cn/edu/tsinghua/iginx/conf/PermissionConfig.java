package cn.edu.tsinghua.iginx.conf;

import java.io.FilePermission;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;

public class PermissionConfig {
  private final static String PERMISSION_CONFIG_FILE = "permission.properties";
  private final static String FILE_CONFIG_PREFIX = "file";

  private PathMatcher getPathMatcher(String pattern) {
    return getFileSystem().getPathMatcher(getPathMatcherSyntax() + ":" + pattern);
  }

  private FileSystem getFileSystem() {
    return FileSystems.getDefault();
  }

  private String getPathMatcherSyntax() {
    return "glob";
  }

  public static class FilePermissionDescriptor {
    private final PathMatcher pathMatcher;
    private final boolean read;
    private final boolean write;
    private final boolean execute;

    public FilePermissionDescriptor(PathMatcher pathMatcher, boolean read, boolean write, boolean execute) {
      this.pathMatcher = pathMatcher;
      this.read = read;
      this.write = write;
      this.execute = execute;
    }

    public PathMatcher getPathMatcher() {
      return pathMatcher;
    }

    public boolean isReadable() {
      return read;
    }

    public boolean isWritable() {
      return write;
    }

    public boolean isExecutable() {
      return execute;
    }
  }
}
