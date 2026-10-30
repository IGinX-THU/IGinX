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

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

final class S3FileAttributes implements BasicFileAttributes {

  private static final FileTime EPOCH = FileTime.fromMillis(0);

  private final boolean directory;
  private final long size;
  private final FileTime lastModified;
  private final Object key;

  private S3FileAttributes(boolean directory, long size, FileTime lastModified, Object key) {
    this.directory = directory;
    this.size = size;
    this.lastModified = lastModified == null ? EPOCH : lastModified;
    this.key = key;
  }

  static S3FileAttributes directory(Object key) {
    return new S3FileAttributes(true, 0, EPOCH, key);
  }

  static S3FileAttributes file(Object key, long size, FileTime lastModified) {
    return new S3FileAttributes(false, size, lastModified, key);
  }

  @Override
  public FileTime lastModifiedTime() {
    return lastModified;
  }

  @Override
  public FileTime lastAccessTime() {
    return lastModified;
  }

  @Override
  public FileTime creationTime() {
    return lastModified;
  }

  @Override
  public boolean isRegularFile() {
    return !directory;
  }

  @Override
  public boolean isDirectory() {
    return directory;
  }

  @Override
  public boolean isSymbolicLink() {
    return false;
  }

  @Override
  public boolean isOther() {
    return false;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public Object fileKey() {
    return key;
  }
}
