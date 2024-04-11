package cn.edu.tsinghua.iginx.transform.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mutex {
  private static final Logger LOGGER = LoggerFactory.getLogger(Mutex.class);

  private boolean isLocked = false;

  public synchronized void lock() {
    while (this.isLocked) {
      try {
        wait();
      } catch (InterruptedException e) {
        LOGGER.error("Mutex was interrupted");
      }
    }
    this.isLocked = true;
  }

  public synchronized void unlock() {
    this.isLocked = false;
    this.notify();
  }
}
