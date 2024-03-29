package cn.edu.tsinghua.iginx.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

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
  public void t1() {
    String path = "test.a{s=2,a=6}";
    StringBuilder name = new StringBuilder("test.a");
    Map<String, String> ret = new LinkedHashMap<>();
    fill(path, name, ret);
    System.out.println(ret);
  }

  @Test
  public void t2() {
    String path = "test.a{s=2,a=6}";
    StringBuilder name = new StringBuilder("test.a");
    Map<String, String> ret = new TreeMap<>();
    fill(path, name, ret);
    System.out.println(ret);
  }

  public void fill(String path, StringBuilder name, Map<String, String> ret) {

    int firstBrace = path.indexOf("{");
    int lastBrace = path.indexOf("}");
    if (firstBrace == -1 || lastBrace == -1) {
      name.append(path);
      return;
    }
    name.append(path, 0, firstBrace);
    String tagLists = path.substring(firstBrace + 1, lastBrace);
    String[] splitPaths = tagLists.split(",");
    for (String tag : splitPaths) {
      int equalPos = tag.indexOf("=");
      String tagKey = tag.substring(0, equalPos);
      String tagVal = tag.substring(equalPos + 1);
      ret.put(tagKey, tagVal);
    }
  }
}
