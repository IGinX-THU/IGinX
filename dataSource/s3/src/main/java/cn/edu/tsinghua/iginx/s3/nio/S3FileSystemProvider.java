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
package cn.edu.tsinghua.iginx.s3.nio;

import cn.edu.tsinghua.iginx.s3.common.S3Object;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.*;
import javax.annotation.Nullable;

/** Read-only provider of {@link S3FileSystem}. Instances are bound to one file system. */
public final class S3FileSystemProvider extends FileSystemProvider {

  public static final String SCHEME = "s3";

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
    throw new UnsupportedOperationException("create S3FileSystem directly");
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    throw new UnsupportedOperationException("create S3FileSystem directly");
  }

  @Override
  public Path getPath(URI uri) {
    throw new UnsupportedOperationException("create S3FileSystem directly");
  }

  @Override
  public SeekableByteChannel newByteChannel(
      Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    for (OpenOption option : options) {
      if (option != StandardOpenOption.READ && option != LinkOption.NOFOLLOW_LINKS) {
        throw new ReadOnlyFileSystemException();
      }
    }
    S3Path s3Path = toS3Path(path);
    S3FileAttributes attributes = readS3Attributes(s3Path);
    if (attributes.isDirectory()) {
      throw new FileSystemException(path.toString(), null, "is a directory");
    }
    return new S3SeekableByteChannel(
        s3Path.getFileSystem(), s3Path.getBucket(), s3Path.getKey(), attributes.size());
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(
      Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
    S3Path s3Dir = toS3Path(dir);
    S3FileSystem fs = s3Dir.getFileSystem();
    List<Path> children = new ArrayList<>();
    if (s3Dir.isRoot()) {
      for (String bucket : fs.client().listBuckets()) {
        if (fs.hiddenBuckets().contains(bucket)) {
          continue;
        }
        S3Path child = (S3Path) s3Dir.resolve(bucket);
        fs.attributesCache().put(child, S3FileAttributes.directory(child.toString()));
        children.add(child);
      }
    } else {
      if (!readS3Attributes(s3Dir).isDirectory()) {
        throw new NotDirectoryException(dir.toString());
      }
      String bucket = s3Dir.getBucket();
      String prefix = s3Dir.getKey().isEmpty() ? "" : s3Dir.getKey() + S3Path.SEPARATOR;
      Map<String, S3FileAttributes> entries = new LinkedHashMap<>();
      for (S3Object object : fs.client().listObjects(bucket, prefix, false)) {
        String name = object.key().substring(prefix.length());
        boolean isDir = object.isDir() || name.endsWith(S3Path.SEPARATOR);
        if (name.endsWith(S3Path.SEPARATOR)) {
          name = name.substring(0, name.length() - 1);
        }
        if (name.isEmpty() || name.contains(S3Path.SEPARATOR)) {
          continue;
        }
        String objectKey = prefix + name;
        if (isDir) {
          entries.putIfAbsent(name, S3FileAttributes.directory(bucket + "/" + objectKey));
        } else {
          entries.put(
              name,
              S3FileAttributes.file(
                  bucket + "/" + objectKey, object.size(), toFileTime(object.lastModified())));
        }
      }
      for (Map.Entry<String, S3FileAttributes> entry : entries.entrySet()) {
        S3Path child = (S3Path) s3Dir.resolve(entry.getKey());
        fs.attributesCache().put(child, entry.getValue());
        children.add(child);
      }
    }

    List<Path> accepted = new ArrayList<>();
    for (Path child : children) {
      if (filter == null || filter.accept(child)) {
        accepted.add(child);
      }
    }
    return new DirectoryStream<Path>() {
      private boolean iterated = false;

      @Override
      public Iterator<Path> iterator() {
        if (iterated) {
          throw new IllegalStateException("iterator already obtained");
        }
        iterated = true;
        return accepted.iterator();
      }

      @Override
      public void close() {}
    };
  }

  S3FileAttributes readS3Attributes(S3Path path) throws IOException {
    S3Path absolute = (S3Path) path.toAbsolutePath().normalize();
    S3FileSystem fs = absolute.getFileSystem();
    S3FileAttributes cached = fs.attributesCache().getIfPresent(absolute);
    if (cached != null) {
      return cached;
    }
    S3FileAttributes attributes = loadAttributes(absolute);
    fs.attributesCache().put(absolute, attributes);
    return attributes;
  }

  private static S3FileAttributes loadAttributes(S3Path path) throws IOException {
    S3FileSystem fs = path.getFileSystem();
    if (path.isRoot()) {
      return S3FileAttributes.directory("/");
    }
    String bucket = path.getBucket();
    String key = path.getKey();
    if (fs.hiddenBuckets().contains(bucket)) {
      throw new NoSuchFileException(path.toString());
    }
    if (key.isEmpty()) {
      if (!fs.client().bucketExists(bucket)) {
        throw new NoSuchFileException(path.toString());
      }
      return S3FileAttributes.directory(path.toString());
    }

    S3Object stat = fs.client().statObjectOrNull(bucket, key);
    if (stat != null) {
      return S3FileAttributes.file(path.toString(), stat.size(), toFileTime(stat.lastModified()));
    }

    try {
      if (fs.client().hasObjectWithPrefix(bucket, key + S3Path.SEPARATOR)) {
        return S3FileAttributes.directory(path.toString());
      }
    } catch (NoSuchFileException e) {
      // the bucket is gone, treated as not found below
    }
    throw new NoSuchFileException(path.toString());
  }

  @Nullable
  private static FileTime toFileTime(@Nullable Instant instant) {
    return instant == null ? null : FileTime.from(instant);
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) {
    throw new ReadOnlyFileSystemException();
  }

  @Override
  public void delete(Path path) {
    throw new ReadOnlyFileSystemException();
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) {
    throw new ReadOnlyFileSystemException();
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) {
    throw new ReadOnlyFileSystemException();
  }

  @Override
  public boolean isSameFile(Path path, Path path2) {
    return path.toAbsolutePath().normalize().equals(path2.toAbsolutePath().normalize());
  }

  @Override
  public boolean isHidden(Path path) {
    return false;
  }

  @Override
  public FileStore getFileStore(Path path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    for (AccessMode mode : modes) {
      if (mode != AccessMode.READ) {
        throw new AccessDeniedException(path.toString());
      }
    }
    readS3Attributes(toS3Path(path));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V extends FileAttributeView> V getFileAttributeView(
      Path path, Class<V> type, LinkOption... options) {
    if (type != BasicFileAttributeView.class) {
      return null;
    }
    S3Path s3Path = toS3Path(path);
    return (V)
        new BasicFileAttributeView() {
          @Override
          public String name() {
            return "basic";
          }

          @Override
          public BasicFileAttributes readAttributes() throws IOException {
            return readS3Attributes(s3Path);
          }

          @Override
          public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime c) {
            throw new ReadOnlyFileSystemException();
          }
        };
  }

  @Override
  @SuppressWarnings("unchecked")
  public <A extends BasicFileAttributes> A readAttributes(
      Path path, Class<A> type, LinkOption... options) throws IOException {
    if (!type.isAssignableFrom(S3FileAttributes.class)) {
      throw new UnsupportedOperationException("unsupported attributes: " + type);
    }
    return (A) readS3Attributes(toS3Path(path));
  }

  @Override
  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
      throws IOException {
    S3FileAttributes attrs = readS3Attributes(toS3Path(path));
    Map<String, Object> result = new HashMap<>();
    result.put("size", attrs.size());
    result.put("isDirectory", attrs.isDirectory());
    result.put("isRegularFile", attrs.isRegularFile());
    result.put("isSymbolicLink", false);
    result.put("isOther", false);
    result.put("lastModifiedTime", attrs.lastModifiedTime());
    result.put("lastAccessTime", attrs.lastAccessTime());
    result.put("creationTime", attrs.creationTime());
    result.put("fileKey", attrs.fileKey());
    return result;
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {
    throw new ReadOnlyFileSystemException();
  }

  private static S3Path toS3Path(Path path) {
    if (!(path instanceof S3Path)) {
      throw new ProviderMismatchException();
    }
    return (S3Path) path;
  }
}
