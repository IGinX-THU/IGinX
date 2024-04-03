package cn.edu.tsinghua.iginx.conf.entity;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;

import javax.annotation.Nullable;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FilePermissionDescriptor {
  private final String username;
  private final Module module;
  private final String pattern;
  private final PathMatcher pathMatcher;
  private final Map<FileAccessType, Boolean> accessMap;

  public FilePermissionDescriptor(@Nullable String username, Module module, String pattern, Map<FileAccessType, Boolean> accessMap) {
    Objects.requireNonNull(module);
    Objects.requireNonNull(pattern);
    Objects.requireNonNull(accessMap);
    this.username = username;
    this.module = module;
    this.pattern = pattern;
    this.pathMatcher = FileSystems.getDefault().getPathMatcher(pattern);
    this.accessMap = Collections.unmodifiableMap(new HashMap<>(accessMap));
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  public Module getModule() {
    return module;
  }

  public String getPattern() {
    return pattern;
  }

  public PathMatcher getPathMatcher() {
    return pathMatcher;
  }

  public Map<FileAccessType, Boolean> getAccessMap() {
    return accessMap;
  }
}
