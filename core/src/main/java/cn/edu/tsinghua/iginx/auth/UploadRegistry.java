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
package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.thrift.FileChunk;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

/** Session-owned uploads stored under server-generated names in a private directory. */
public final class UploadRegistry {
  private static final UploadRegistry INSTANCE = new UploadRegistry();
  private final Map<String, Upload> uploads = new HashMap<>();
  private Path directory;

  public static UploadRegistry getInstance() {
    return INSTANCE;
  }

  public static boolean validName(String name) {
    return name != null
        && !name.isEmpty()
        && !name.contains("..")
        && !name.contains("/")
        && !name.contains("\\")
        && !name.contains(":")
        && name.indexOf('\0') < 0;
  }

  public synchronized boolean owns(long session, String name) {
    Upload upload = uploads.get(name);
    return validName(name) && upload != null && upload.session == session;
  }

  public synchronized Path path(long session, String name) {
    if (!owns(session, name)) throw new SecurityException("Upload does not belong to session");
    return uploads.get(name).path;
  }

  public synchronized Status write(long session, FileChunk chunk) {
    String name = chunk.getFileName();
    if (!validName(name) || chunk.getOffset() < 0 || chunk.bufferForData() == null)
      return RpcUtils.ACCESS_DENY;
    Upload upload = uploads.get(name);
    if (upload != null && upload.session != session) return RpcUtils.ACCESS_DENY;
    try {
      if (upload == null) {
        if (directory == null) directory = Files.createTempDirectory("iginx-uploads-");
        upload = new Upload(session, Files.createTempFile(directory, "upload-", ".csv"));
        uploads.put(name, upload);
      }
      try (FileChannel file = FileChannel.open(upload.path, StandardOpenOption.WRITE)) {
        file.position(chunk.getOffset());
        ByteBuffer data = chunk.bufferForData().duplicate();
        while (data.hasRemaining()) file.write(data);
      }
      return RpcUtils.SUCCESS;
    } catch (IOException e) {
      return new Status(RpcUtils.FAILURE).setMessage("File chunk upload failed");
    }
  }

  public synchronized void release(long session, String name) throws IOException {
    if (owns(session, name)) {
      Files.deleteIfExists(uploads.get(name).path);
      uploads.remove(name);
    }
  }

  public synchronized void releaseSession(long session) {
    java.util.Iterator<Map.Entry<String, Upload>> iterator = uploads.entrySet().iterator();
    while (iterator.hasNext()) {
      Upload upload = iterator.next().getValue();
      if (upload.session != session) continue;
      try {
        Files.deleteIfExists(upload.path);
      } catch (IOException e) {
        org.slf4j.LoggerFactory.getLogger(UploadRegistry.class)
            .warn("Could not remove expired upload", e);
      }
      iterator.remove();
    }
  }

  private static final class Upload {
    private final long session;
    private final Path path;

    private Upload(long session, Path path) {
      this.session = session;
      this.path = path;
    }
  }
}
