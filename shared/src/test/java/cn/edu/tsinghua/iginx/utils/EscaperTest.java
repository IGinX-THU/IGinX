package cn.edu.tsinghua.iginx.utils;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class EscaperTest {

  @Test
  public void escape() {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put('$', '!');
    Escaper escaper = new Escaper('\\', replacementMap);

    assertEquals("hello\\!world", escaper.escape("hello$world"));
    assertEquals("hello\\\\world", escaper.escape("hello\\world"));
  }

  @Test
  public void unescape() throws ParseException {
    Map<Character, Character> replacementMap = new HashMap<>();
    replacementMap.put('\\', '\\');
    replacementMap.put('$', '!');
    Escaper escaper = new Escaper('\\', replacementMap);

    assertEquals("hello$world", escaper.unescape("hello\\!world"));
    assertEquals("hello\\world", escaper.unescape("hello\\\\world"));
  }
}
