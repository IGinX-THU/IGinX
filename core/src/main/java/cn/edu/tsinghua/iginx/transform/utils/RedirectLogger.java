package cn.edu.tsinghua.iginx.transform.utils;

import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedirectLogger extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedirectLogger.class);

  private final InputStream inputStream;

  private final String name;

  public RedirectLogger(InputStream inputStream, String name) {
    this.inputStream = inputStream;
    this.name = name;
  }

  @Override
  public void run() {
    LOGGER.info("hello");
    //        Scanner scanner = new Scanner(inputStream);
    //        while (scanner.hasNextLine()) {
    //            LOGGER.info(String.format("[Python %s] ", name) + scanner.nextLine());
    //        }
    try {
      byte[] buffer = new byte[1024];
      int len = -1;
      while ((len = inputStream.read(buffer)) > 0) {
        System.out.write(buffer, 0, len);
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
  }
}
