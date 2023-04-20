package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ShellRunner {
    public void runShellCommand(String command) throws Exception {
        Process p = null;
        try {
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(true);
            p = builder.start();
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

            int status = p.waitFor();
            System.err.printf(
                    "runShellCommand: %s, status: %s%n, %s%n", command, p.exitValue(), status);
            if (p.exitValue() != 0) {
                throw new Exception("tests fail!");
            }
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
    }
}
