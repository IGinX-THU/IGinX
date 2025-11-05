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
package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.io.*;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientLauncher.class);

  private final Process process;
  private final PrintWriter writer;
  private final ExecutorService executor;
  private final LinkedBlockingQueue<String> outputQueue = new LinkedBlockingQueue<>();
  private final StringBuffer resultBuffer = new StringBuffer();

  private static final Pattern TIME_COST_PATTERN = Pattern.compile("Time cost:\\s*\\d+\\s*ms");

  public ClientLauncher() {
    Process tempProcess = null;
    PrintWriter tempWriter = null;
    ExecutorService tempExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "ClientOutputReader");
              t.setDaemon(true);
              return t;
            });

    try {
      // Locate client version directory
      File clientDir = new File("../client/target/");
      File[] matchingFiles = clientDir.listFiles((dir, name) -> name.startsWith("iginx-client-"));
      if (matchingFiles == null || matchingFiles.length == 0) {
        throw new IOException(
            "No iginx-client-* directory found in " + clientDir.getAbsolutePath());
      }

      matchingFiles = Arrays.stream(matchingFiles).filter(File::isDirectory).toArray(File[]::new);
      if (matchingFiles.length == 0) {
        throw new IOException("No valid iginx-client-* directory found.");
      }

      String version = matchingFiles[0].getName();
      String clientUnixPath = "../client/target/" + version + "/sbin/start_cli.sh";
      String clientWinPath =
          new File("../client/target/" + version + "/sbin/start_cli.bat").getCanonicalPath();

      // Start CLI client
      ProcessBuilder pb = new ProcessBuilder();
      if (ShellRunner.isOnWin()) {
        pb.command("cmd.exe", "/c", clientWinPath);
      } else {
        Process before = Runtime.getRuntime().exec(new String[] {"chmod", "+x", clientUnixPath});
        before.waitFor();
        pb.command("bash", "-c", clientUnixPath);
      }

      pb.redirectErrorStream(true);
      tempProcess = pb.start();

      tempWriter = new PrintWriter(new OutputStreamWriter(tempProcess.getOutputStream()), true);
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(tempProcess.getInputStream()));

      // Read output asynchronously
      Process finalProcess = tempProcess;
      tempExecutor.submit(
          () -> {
            try {
              String line;
              while ((line = reader.readLine()) != null) {
                outputQueue.offer(line);
              }
            } catch (IOException e) {
              if (finalProcess.isAlive()) {
                LOGGER.error("Failed to read client output: ", e);
              } else {
                LOGGER.debug("Client process has exited, stop reading output.");
              }
            }
          });

      // Give CLI some time to initialize
      Thread.sleep(2000);
      if (tempProcess.isAlive()) {
        LOGGER.info("Client process started successfully.");
      } else {
        LOGGER.warn("Client process exited unexpectedly after startup.");
      }
    } catch (Exception e) {
      LOGGER.error("Failed to launch IginX client: ", e);
      tempExecutor.shutdownNow();
    }

    this.process = tempProcess;
    this.writer = tempWriter;
    this.executor = tempExecutor;
  }

  public void readLine(String command) {
    this.readLine(command, 0);
  }

  /** Execute a single command and capture its output. */
  public void readLine(String command, int timeoutMs) {
    if (process == null || writer == null) {
      LOGGER.error("Client process is not initialized.");
      return;
    }
    if (!process.isAlive()) {
      LOGGER.error("Client process has already exited.");
      return;
    }

    LOGGER.info("Executing command: {}", command);
    resultBuffer.setLength(0);
    outputQueue.clear();

    writer.println(command);
    writer.flush();

    StringBuilder output = new StringBuilder();
    long startTime = System.currentTimeMillis();

    try {
      while (true) {
        if (timeoutMs > 0 && (System.currentTimeMillis() - startTime > timeoutMs)) {
          LOGGER.warn("Command '{}' timed out after {} ms.", command, timeoutMs);
          break;
        }

        if (!process.isAlive()) {
          LOGGER.warn(
              "Process exited unexpectedly while waiting for command '{}' to finish.", command);
          break; // 进程意外退出，退出循环
        }

        String line = outputQueue.poll(500, TimeUnit.MILLISECONDS);

        if (line != null) {
          // 检查是否是时间日志，表示命令执行完毕
          if (TIME_COST_PATTERN.matcher(line.trim()).find()) {
            LOGGER.debug("Detected time cost log, command finished.");
            output.append(line).append(System.lineSeparator());
            break;
          }

          // 否则，将输出追加到结果中（过滤掉发出的命令）
          if (!line.trim().isEmpty() && !line.contains(command)) {
            output.append(line).append(System.lineSeparator());
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted while reading command output: ", e);
    }

    resultBuffer.append(output.toString().trim());
  }

  /** Get the command execution result. */
  public String getResult() {
    return resultBuffer.toString();
  }

  public boolean expectedOutputContains(String result) {
    String ret = resultBuffer.toString();
    if (ret.contains(result)) {
      return true;
    } else {
      LOGGER.error("Expected output: {}, actually output: {}", result, ret);
      return false;
    }
  }

  /** Close the client process gracefully. */
  public void close() {
    if (writer == null || process == null) {
      LOGGER.warn("Client process not initialized or already closed.");
      return;
    }

    try {
      writer.println("exit;");
      writer.flush();
      Thread.sleep(200);

      if (process.isAlive()) {
        process.destroy();
        if (!process.waitFor(1, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          LOGGER.warn("Client process did not exit gracefully, force killed.");
        }
      }

      executor.shutdownNow();
      LOGGER.info("Client process closed successfully.");
    } catch (Exception e) {
      LOGGER.error("Error while closing client process: ", e);
    }
  }
}
