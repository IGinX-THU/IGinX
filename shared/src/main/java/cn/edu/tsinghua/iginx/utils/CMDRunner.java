package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/** to run cmd commands on WindowsOS */
public class CMDRunner {
  public static void runShellCommand(String... command) throws Exception {
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

      int status = p.waitFor();
      System.err.printf(
          "runCMDCommand: %s, status: %s%n, %s%n", Arrays.toString(command), p.exitValue(), status);
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
}
