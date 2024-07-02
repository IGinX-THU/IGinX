/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.transform.driver;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.ArrowReader;
import cn.edu.tsinghua.iginx.transform.data.BatchData;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPCWorker extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(IPCWorker.class);

  private final long pid;

  private final String ip;

  private final int javaPort;

  private final int pyPort;

  private final Process process;

  private final ServerSocket serverSocket;

  private final Writer writer;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private final ExecutorService threadPool = Executors.newFixedThreadPool(5);

  public IPCWorker(
      long pid,
      int javaPort,
      int pyPort,
      Process process,
      ServerSocket serverSocket,
      Writer writer) {
    this.pid = pid;
    this.ip = config.getIp();
    this.javaPort = javaPort;
    this.pyPort = pyPort;
    this.process = process;
    this.serverSocket = serverSocket;
    this.writer = writer;
  }

  @Override
  public void run() {
    try {
      while (true) {
        Socket socket = serverSocket.accept();
        threadPool.submit(() -> process(socket));
      }
    } catch (SocketException ignored) {
      LOGGER.info("{} stop server socket.", this);
    } catch (IOException e) {
      throw new RuntimeException("An error occurred while listening.", e);
    }
  }

  public void process(Socket socket) {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
      VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
      reader.loadNextBatch();

      Reader arrowReader = new ArrowReader(readBatch, config.getBatchSize());
      while (arrowReader.hasNextBatch()) {
        BatchData batchData = arrowReader.loadNextBatch();
        writer.writeBatch(batchData);
      }

      reader.close();
      socket.close();
    } catch (IOException | WriteBatchException e) {
      LOGGER.error("Worker pid={} fail to process socket.", pid);
      throw new RuntimeException("Fail to process socket", e);
    }
  }

  public void close() {
    if (process.isAlive()) {
      this.process.destroy();
    }
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        this.serverSocket.close();
      } catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }
    threadPool.shutdown();
  }

  public long getPid() {
    return pid;
  }

  public int getPyPort() {
    return pyPort;
  }

  public Process getProcess() {
    return process;
  }

  @Override
  public String toString() {
    return "Worker{"
        + "pid="
        + pid
        + ", ip='"
        + ip
        + '\''
        + ", javaPort="
        + javaPort
        + ", pyPort="
        + pyPort
        + ", process="
        + process
        + ", serverSocket="
        + serverSocket
        + ", writer="
        + writer
        + ", threadPool="
        + threadPool
        + '}';
  }
}
