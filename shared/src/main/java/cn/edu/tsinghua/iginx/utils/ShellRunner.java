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
package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ShellRunner {

  // to run .sh script on WindowsOS in github action tests
  // bash.exe path in action windows runners
  public static final String BASH_PATH = "C:/Program Files/Git/bin/bash.exe";

  public void runShellCommand(String command) throws Exception {
    Process p = null;
    try {
      ProcessBuilder builder = new ProcessBuilder();
      if (isOnWin()) {
        builder.command(BASH_PATH, command); // WSL负责安装docker环境,默认使用Git Bash,WSL Bash可能会缺少依赖的基础环境。
      } else {
        builder.command(command);
      }
      builder.redirectErrorStream(true);
      p = builder.start();
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }

      int status = p.waitFor();
      System.err.printf("runShellCommand: %s, status: %s%n, %s%n", command, p.exitValue(), status);
      if (p.exitValue() != 0) {
        throw new Exception("tests fail!");
      }
    } finally {
      if (p != null) {
        p.destroy();
      }
    }
  }

  // to directly run command(compare to scripts)
  public static void runCommand(String... command) throws Exception {
    Process p = null;
    try {
      ProcessBuilder builder = new ProcessBuilder();
      builder.command(command);
      builder.redirectErrorStream(true);
      p = builder.start();
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }

      p.waitFor();
      int i = p.exitValue();
      if (i != 0) {
        throw new Exception(
            "process exited with value: " + i + "; command: " + Arrays.toString(command));
      }
    } catch (IOException | SecurityException e) {
      throw new Exception("run command failed: " + e.getMessage());
    } finally {
      if (p != null) {
        p.destroy();
      }
    }
  }

  public static boolean isOnWin() {
    return System.getProperty("os.name").toLowerCase().contains("win");
  }

  // allow using customized bash path on local windows
  // if local os has customized bash path then don't need to use BASH_PATH
  public static boolean isCommandOnPath(String command) {
    try {
      Process process = new ProcessBuilder(command, "--version").start();
      int exitCode = process.waitFor();
      return exitCode == 0;
    } catch (IOException | InterruptedException e) {
      return false;
    }
  }
}
