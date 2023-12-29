package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ShellRunner {

  // to run .sh script on WindowsOS in github action tests
  // bash.exe path in action windows runners
  public static final String BASH_PATH = "C:/Program Files/Git/bin/bash.exe";

  public void runShellCommand(String command) throws Exception {
    Process p = null;
    try {
      ProcessBuilder builder = new ProcessBuilder();
      if (isOnWin()) {;
        builder.command((isCommandOnPath("bash") ? "bash" : BASH_PATH), command);
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
