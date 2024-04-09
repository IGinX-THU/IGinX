package cn.edu.tsinghua.iginx.utils;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class EscaperTest {

  @Test
  public void escape() {
    {
      Map<Character, Character> replacementMap = new HashMap<>();
      replacementMap.put('\\', '\\');
      replacementMap.put('$', '!');
      Escaper escaper = new Escaper('\\', replacementMap);

      assertEquals("hello\\!world", escaper.escape("hello$world"));
      assertEquals("hello\\\\world", escaper.escape("hello\\world"));
    }

    try {
      Map<Character, Character> replacementMap = new HashMap<>();
      replacementMap.put('$', '!');
      Escaper escaper = new Escaper('\\', replacementMap);
      escaper.escape("hello\\world");
      fail("should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("replacementMap should contain escape character", e.getMessage());
    }
  }

  @Test
  public void unescape() throws ParseException {
    {
      Map<Character, Character> replacementMap = new HashMap<>();
      replacementMap.put('\\', '\\');
      replacementMap.put('$', '!');
      Escaper escaper = new Escaper('\\', replacementMap);

      assertEquals("hello$world", escaper.unescape("hello\\!world"));
      assertEquals("hello\\world", escaper.unescape("hello\\\\world"));
    }

    try {
      Map<Character, Character> replacementMap = new HashMap<>();
      replacementMap.put('$', '!');
      Escaper escaper = new Escaper('\\', replacementMap);
      escaper.unescape("hello\\world");
      fail("should throw ParseException");
    } catch (Exception e) {
      assertEquals("Invalid escape character", e.getMessage());
    }
  }
}
