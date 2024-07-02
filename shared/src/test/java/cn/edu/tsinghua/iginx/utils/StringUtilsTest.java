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

  @Test
  public void toRegexExpr() {
    assertEquals("\\Qa.b.c\\E", StringUtils.toRegexExpr("a.b.c"));
    assertEquals(".*", StringUtils.toRegexExpr("*"));
    assertEquals(".*\\Q.b.c\\E", StringUtils.toRegexExpr("*.b.c"));
    assertEquals("\\Qa.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("a.*.c"));
    assertEquals("\\Qa.b.\\E.*", StringUtils.toRegexExpr("a.b.*"));
    assertEquals("\\Qa.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("a.*.*"));
    assertEquals(".*\\Q.b.\\E.*", StringUtils.toRegexExpr("*.b.*"));
    assertEquals(".*\\Q.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("*.*.c"));
    assertEquals(".*\\Q.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("*.*.*"));
    assertEquals(".*", StringUtils.toRegexExpr("**"));
    assertEquals(".*\\Q.b.c\\E", StringUtils.toRegexExpr("**.b.c"));
    assertEquals("\\Qa.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("a.**.c"));
    assertEquals("\\Qa.b.\\E.*", StringUtils.toRegexExpr("a.b.**"));
    assertEquals("\\Qa.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("a.**.**"));
    assertEquals(".*\\Q.b.\\E.*", StringUtils.toRegexExpr("**.b.**"));
    assertEquals(".*\\Q.\\E.*\\Q.c\\E", StringUtils.toRegexExpr("**.**.c"));
    assertEquals(".*\\Q.\\E.*\\Q.\\E.*", StringUtils.toRegexExpr("**.**.**"));
  }
}
