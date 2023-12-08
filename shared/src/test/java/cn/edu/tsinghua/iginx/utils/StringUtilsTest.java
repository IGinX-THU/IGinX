package cn.edu.tsinghua.iginx.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class StringUtilsTest {

  @Before
  public void setUp() {}

  @Test
  public void reformatPathTest() {
    String str = ".^${}+?()[]|\\\\*";
    String result = StringUtils.reformatPath(str);
    assertEquals(result, "\\.\\^\\$\\{\\}\\+\\?\\(\\)\\[\\]\\|\\\\\\\\.*");
  }
}
