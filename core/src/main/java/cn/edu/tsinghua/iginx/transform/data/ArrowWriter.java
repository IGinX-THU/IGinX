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
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.Channels;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowWriter implements Writer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  private final String ip;

  private final int writerPort;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public ArrowWriter(int writerPort) {
    this.ip = config.getIp();
    this.writerPort = writerPort;
  }

  public void writeVector(VectorSchemaRoot root) throws WriteBatchException {
    try {
      Socket socket = new Socket(ip, writerPort);
      OutputStream os = socket.getOutputStream();

      ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));

      writer.start();
      writer.writeBatch();
      writer.end();

      writer.close();
      os.close();
      socket.close();
    } catch (IOException e) {
      LOGGER.error("ArrowWriter fail to write vector");
      throw new WriteBatchException("ArrowWriter fail to write vector", e);
    }
  }

  @Override
  public void writeBatch(BatchData batchData) throws WriteBatchException {
    try {
      Socket socket = new Socket(ip, writerPort);
      OutputStream os = socket.getOutputStream();

      VectorSchemaRoot root = batchData.wrapAsVectorSchemaRoot();
      ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));

      writer.start();
      writer.writeBatch();
      writer.end();

      writer.close();
      os.close();
      socket.close();
    } catch (IOException e) {
      LOGGER.error("ArrowWriter fail to write batch");
      throw new WriteBatchException("ArrowWriter fail to write batch", e);
    }
  }

  @Override
  public void reset() {}
}
