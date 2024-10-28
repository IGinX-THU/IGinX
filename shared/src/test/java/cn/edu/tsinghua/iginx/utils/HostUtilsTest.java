package cn.edu.tsinghua.iginx.utils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HostUtilsTest.class);

  @Test
  public void test() {
    LOGGER.info("IP: {}", HostUtils.getRepresentativeIP());
  }
}
