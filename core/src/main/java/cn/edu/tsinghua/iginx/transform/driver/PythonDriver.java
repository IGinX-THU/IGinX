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
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TransformTaskMeta;
import cn.edu.tsinghua.iginx.transform.api.Driver;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.exception.CreateWorkerException;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.utils.Constants;
import cn.edu.tsinghua.iginx.transform.utils.RedirectLogger;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonDriver implements Driver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PythonDriver.class);

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PYTHON_CMD = config.getPythonCMD();

  private static final String PYTHON_DIR = config.getDefaultUDFDir();

  private static final String PY_WORKER =
      File.separator + "python_scripts" + File.separator + "py_worker.py";

  private static final String PY_SUFFIX = ".py";

  private static final int TEST_WAIT_TIME = 10000;

  private static PythonDriver instance;

  private PythonDriver() {
    File file = new File(PYTHON_DIR + PY_WORKER);
    if (!file.exists()) {
      LOGGER.error("Python driver file didn't exists.");
    }
  }

  public static PythonDriver getInstance() {
    if (instance == null) {
      synchronized (PythonDriver.class) {
        if (instance == null) {
          instance = new PythonDriver();
        }
      }
    }
    return instance;
  }

  @Override
  public IPCWorker createWorker(PythonTask task, Writer writer) throws TransformException {
    String name = task.getPyTaskName();

    TransformTaskMeta taskMeta = metaManager.getTransformTask(name);
    if (taskMeta == null) {
      throw new CreateWorkerException(
          String.format("Fail to load task info by task name: %s", name));
    }
    if (!taskMeta.getIpSet().contains(config.getIp())) {
      throw new CreateWorkerException(
          String.format(
              "Fail to load task file, because current ip is: %s, and register ip is: %s",
              config.getIp(), config.getIp()));
    }

    String fileName = taskMeta.getFileName();
    String className = taskMeta.getClassName();
    String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));

    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(new byte[] {127, 0, 0, 1}));
      int javaPort = serverSocket.getLocalPort();

      ProcessBuilder pb = new ProcessBuilder();
      pb.inheritIO()
          .command(
              PYTHON_CMD, PYTHON_DIR + PY_WORKER, moduleName, className, String.valueOf(javaPort));
      Process process = pb.start();

      // Redirect worker process stdout and stderr
      //            redirectStreamsToLogger(process.getInputStream(),
      // process.getErrorStream());

      // Wait for it to connect to our socket.
      //            serverSocket.setSoTimeout(TEST_WAIT_TIME);

      Socket socket = serverSocket.accept();

      RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      try (ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
        reader.loadNextBatch();

        BigIntVector pidVector = (BigIntVector) readBatch.getVector(0);
        long pid = pidVector.get(0);
        BigIntVector portVector = (BigIntVector) readBatch.getVector(1);
        int pyPort = (int) portVector.get(0);
        BigIntVector statusVector = (BigIntVector) readBatch.getVector(2);
        int status = (int) statusVector.get(0);

        socket.close();

        if (pid < 0) {
          throw new CreateWorkerException(
              String.format("Failed to launch python worker with pid=%d", pid));
        } else if (status < 0) {
          throw new CreateWorkerException(
              String.format(
                  "Failed to launch python worker with status=%s",
                  Constants.getWorkerStatusInfo(status)));
        } else {
          IPCWorker IPCWorker = new IPCWorker(pid, javaPort, pyPort, process, serverSocket, writer);
          LOGGER.info("{} has started.", IPCWorker);
          return IPCWorker;
        }
      }
    } catch (IOException e) {
      throw new CreateWorkerException("Failed to launch python worker", e);
    }
  }

  public boolean testWorker(String fileName, String className) {
    ServerSocket serverSocket = null;
    Process process = null;
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(new byte[] {127, 0, 0, 1}));
      int javaPort = serverSocket.getLocalPort();
      String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));

      ProcessBuilder pb = new ProcessBuilder();
      pb.inheritIO()
          .command(
              PYTHON_CMD, PYTHON_DIR + PY_WORKER, moduleName, className, String.valueOf(javaPort));
      process = pb.start();

      // Redirect worker process stdout and stderr
      //            redirectStreamsToLogger(process.getInputStream(),
      // process.getErrorStream());

      // Wait for it to connect to our socket.
      serverSocket.setSoTimeout(TEST_WAIT_TIME);

      Socket socket = serverSocket.accept();
      RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      try (ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
        reader.loadNextBatch();

        BigIntVector pidVector = (BigIntVector) readBatch.getVector(0);
        long pid = pidVector.get(0);

        socket.close();

        if (pid < 0) {
          LOGGER.error("Failed to launch python worker with code={}", pid);
          return false;
        } else {
          LOGGER.info("Worker(pid={}) has started.", pid);
          return true;
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to launch python worker", e);
      return false;
    } finally {
      if (process != null && process.isAlive()) {
        process.destroy();
      }
      if (serverSocket != null && !serverSocket.isClosed()) {
        try {
          serverSocket.close();
        } catch (IOException e) {
          LOGGER.error("Fail to close server socket, because ", e);
        }
      }
    }
  }

  private void redirectStreamsToLogger(InputStream stdout, InputStream stderr) {
    new RedirectLogger(stdout, "stdout").start();
    new RedirectLogger(stderr, "stderr").start();
  }
}
