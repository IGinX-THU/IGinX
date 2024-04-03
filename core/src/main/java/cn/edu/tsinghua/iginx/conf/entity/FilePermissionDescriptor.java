package cn.edu.tsinghua.iginx.conf.entity;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;

import javax.annotation.Nullable;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

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
    this.accessMap = Collections.unmodifiableMap(accessMap.entrySet().stream().
        filter(entry -> entry.getValue() != null).
        collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FilePermissionDescriptor that = (FilePermissionDescriptor) o;
    return Objects.equals(username, that.username) && module == that.module && Objects.equals(pattern, that.pattern) && Objects.equals(accessMap, that.accessMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, module, pattern, accessMap);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FilePermissionDescriptor.class.getSimpleName() + "[", "]")
        .add("username='" + username + "'")
        .add("module=" + module)
        .add("pattern='" + pattern + "'")
        .add("accessMap=" + accessMap)
        .toString();
  }
}
