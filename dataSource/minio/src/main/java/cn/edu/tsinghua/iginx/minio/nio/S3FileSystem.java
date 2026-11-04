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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.minio.MinioClient;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Read-only {@link FileSystem} view of a MinIO (S3 compatible) server, on which {@link
 * cn.edu.tsinghua.iginx.minio.tree.ObjectTreeManager} reads objects as files.
 *
 * <p>Objects keys are split by {@code /} into virtual directories. A name is a regular file if an
 * object with that exact key exists, otherwise it is a directory if any object has it as prefix.
 */
public final class S3FileSystem extends FileSystem {

  private final S3FileSystemProvider provider;
  private final MinioClient client;
  private final Set<String> hiddenBuckets;
  private final int blockSize;
  private final Cache<S3Path, S3FileAttributes> attributesCache;
  private final S3Path root;
  private volatile boolean open = true;

  public S3FileSystem(
      MinioClient client, Collection<String> hiddenBuckets, int blockSize, Duration cacheTtl) {
    this.provider = new S3FileSystemProvider();
    this.client = Objects.requireNonNull(client);
    this.hiddenBuckets = Collections.unmodifiableSet(new HashSet<>(hiddenBuckets));
    this.blockSize = blockSize;
    this.attributesCache =
        Caffeine.newBuilder().expireAfterWrite(cacheTtl).maximumSize(100_000).build();
    this.root = new S3Path(this, true, Collections.emptyList());
  }

  MinioClient client() {
    return client;
  }

  Set<String> hiddenBuckets() {
    return hiddenBuckets;
  }

  int blockSize() {
    return blockSize;
  }

  Cache<S3Path, S3FileAttributes> attributesCache() {
    return attributesCache;
  }

  public S3Path root() {
    return root;
  }

  @Override
  public FileSystemProvider provider() {
    return provider;
  }

  @Override
  public void close() {
    open = false;
    attributesCache.invalidateAll();
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public String getSeparator() {
    return S3Path.SEPARATOR;
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    return Collections.singletonList(root);
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    return Collections.emptyList();
  }

  @Override
  public Set<String> supportedFileAttributeViews() {
    return Collections.singleton("basic");
  }

  @Override
  public S3Path getPath(String first, String... more) {
    return S3Path.parse(this, first, more);
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    int pos = syntaxAndPattern.indexOf(':');
    if (pos <= 0) {
      throw new IllegalArgumentException("invalid syntax and pattern: " + syntaxAndPattern);
    }
    String syntax = syntaxAndPattern.substring(0, pos);
    String pattern = syntaxAndPattern.substring(pos + 1);
    if (!syntax.equalsIgnoreCase("regex")) {
      throw new UnsupportedOperationException("only regex syntax is supported");
    }
    Pattern compiled = Pattern.compile(pattern);
    return path -> compiled.matcher(path.toString()).matches();
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    throw new UnsupportedOperationException();
  }

  @Override
  public WatchService newWatchService() {
    throw new UnsupportedOperationException();
  }
}
