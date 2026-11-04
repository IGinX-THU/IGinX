/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.minio.nio;

import java.io.File;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/**
 * Path of {@link S3FileSystem}. An absolute path {@code /bucket/k1/k2} refers to the object (or the
 * virtual directory) whose key is {@code k1/k2} in {@code bucket}; the root {@code /} lists
 * buckets.
 */
public final class S3Path implements Path {

  static final String SEPARATOR = "/";

  private final S3FileSystem fs;
  private final boolean absolute;
  private final List<String> names;

  S3Path(S3FileSystem fs, boolean absolute, List<String> names) {
    this.fs = Objects.requireNonNull(fs);
    this.absolute = absolute;
    this.names = Collections.unmodifiableList(new ArrayList<>(names));
  }

  static S3Path parse(S3FileSystem fs, String first, String... more) {
    StringBuilder sb = new StringBuilder(first);
    for (String s : more) {
      if (s.isEmpty()) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append(SEPARATOR);
      }
      sb.append(s);
    }
    String joined = sb.toString();
    boolean absolute = joined.startsWith(SEPARATOR);
    List<String> names = new ArrayList<>();
    for (String name : joined.split(SEPARATOR)) {
      if (!name.isEmpty()) {
        names.add(name);
      }
    }
    return new S3Path(fs, absolute, names);
  }

  /** @return bucket name, or null if this is root or relative */
  public String getBucket() {
    if (!absolute || names.isEmpty()) {
      return null;
    }
    return names.get(0);
  }

  /** @return object key inside bucket, empty string means the bucket itself */
  public String getKey() {
    if (!absolute || names.size() <= 1) {
      return "";
    }
    return String.join(SEPARATOR, names.subList(1, names.size()));
  }

  boolean isRoot() {
    return absolute && names.isEmpty();
  }

  List<String> names() {
    return names;
  }

  @Override
  public S3FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public boolean isAbsolute() {
    return absolute;
  }

  @Override
  public Path getRoot() {
    return absolute ? new S3Path(fs, true, Collections.emptyList()) : null;
  }

  @Override
  public Path getFileName() {
    if (names.isEmpty()) {
      return absolute ? null : this;
    }
    return new S3Path(fs, false, Collections.singletonList(names.get(names.size() - 1)));
  }

  @Override
  public Path getParent() {
    if (names.isEmpty() || (!absolute && names.size() == 1)) {
      return null;
    }
    return new S3Path(fs, absolute, names.subList(0, names.size() - 1));
  }

  @Override
  public int getNameCount() {
    return names.size();
  }

  @Override
  public Path getName(int index) {
    return new S3Path(fs, false, Collections.singletonList(names.get(index)));
  }

  @Override
  public Path subpath(int beginIndex, int endIndex) {
    return new S3Path(fs, false, names.subList(beginIndex, endIndex));
  }

  @Override
  public boolean startsWith(Path other) {
    if (!(other instanceof S3Path)) {
      return false;
    }
    S3Path that = (S3Path) other;
    if (that.absolute != absolute || that.names.size() > names.size()) {
      return false;
    }
    return names.subList(0, that.names.size()).equals(that.names);
  }

  @Override
  public boolean startsWith(String other) {
    return startsWith(fs.getPath(other));
  }

  @Override
  public boolean endsWith(Path other) {
    if (!(other instanceof S3Path)) {
      return false;
    }
    S3Path that = (S3Path) other;
    if (that.absolute) {
      return equals(that);
    }
    if (that.names.size() > names.size()) {
      return false;
    }
    return names.subList(names.size() - that.names.size(), names.size()).equals(that.names);
  }

  @Override
  public boolean endsWith(String other) {
    return endsWith(fs.getPath(other));
  }

  @Override
  public Path normalize() {
    Deque<String> stack = new ArrayDeque<>();
    for (String name : names) {
      if (name.equals(".")) {
        continue;
      }
      if (name.equals("..")) {
        if (!stack.isEmpty() && !stack.peekLast().equals("..")) {
          stack.pollLast();
          continue;
        }
        if (absolute) {
          continue;
        }
      }
      stack.addLast(name);
    }
    return new S3Path(fs, absolute, new ArrayList<>(stack));
  }

  @Override
  public Path resolve(Path other) {
    S3Path that = checkPath(other);
    if (that.absolute) {
      return that;
    }
    if (that.names.isEmpty()) {
      return this;
    }
    List<String> joined = new ArrayList<>(names);
    joined.addAll(that.names);
    return new S3Path(fs, absolute, joined);
  }

  @Override
  public Path resolve(String other) {
    return resolve(fs.getPath(other));
  }

  @Override
  public Path resolveSibling(Path other) {
    Path parent = getParent();
    return parent == null ? other : parent.resolve(other);
  }

  @Override
  public Path resolveSibling(String other) {
    return resolveSibling(fs.getPath(other));
  }

  @Override
  public Path relativize(Path other) {
    S3Path that = checkPath(other);
    if (that.absolute != absolute) {
      throw new IllegalArgumentException("'other' is different type of Path");
    }
    int common = 0;
    while (common < names.size()
        && common < that.names.size()
        && names.get(common).equals(that.names.get(common))) {
      common++;
    }
    List<String> result = new ArrayList<>();
    for (int i = common; i < names.size(); i++) {
      result.add("..");
    }
    result.addAll(that.names.subList(common, that.names.size()));
    return new S3Path(fs, false, result);
  }

  @Override
  public URI toUri() {
    return URI.create(fs.provider().getScheme() + "://" + toAbsolutePath());
  }

  @Override
  public Path toAbsolutePath() {
    if (absolute) {
      return this;
    }
    return new S3Path(fs, true, names);
  }

  @Override
  public Path toRealPath(LinkOption... options) {
    return toAbsolutePath().normalize();
  }

  @Override
  public File toFile() {
    throw new UnsupportedOperationException("S3Path cannot be converted to java.io.File");
  }

  @Override
  public WatchKey register(
      WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Path> iterator() {
    List<Path> parts = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      parts.add(getName(i));
    }
    return parts.iterator();
  }

  @Override
  public int compareTo(Path other) {
    return toString().compareTo(other.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof S3Path)) {
      return false;
    }
    S3Path that = (S3Path) o;
    return fs == that.fs && absolute == that.absolute && names.equals(that.names);
  }

  @Override
  public int hashCode() {
    return Objects.hash(System.identityHashCode(fs), absolute, names);
  }

  @Override
  public String toString() {
    String joined = String.join(SEPARATOR, names);
    return absolute ? SEPARATOR + joined : joined;
  }

  private S3Path checkPath(Path other) {
    if (!(other instanceof S3Path) || ((S3Path) other).fs != fs) {
      throw new ProviderMismatchException();
    }
    return (S3Path) other;
  }
}
