package cn.edu.tsinghua.iginx.engine.physical.storage.utils;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.utils.Escaper;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

public class ColumnKeyTranslatorTest {

  @Test(expected = IllegalArgumentException.class)
  public void testSameSeparator() {
    Map<Character, Character> map = new HashMap<>();
    map.put('\\', '\\');
    map.put('a', 'a');
    map.put('b', 'b');
    new ColumnKeyTranslator('a', 'a', new Escaper('\\', map));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSameEscapePrefixWithTagSep() {
    Map<Character, Character> map = new HashMap<>();
    map.put('\\', '\\');
    map.put('b', 'b');
    new ColumnKeyTranslator('\\', 'b', new Escaper('\\', map));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSameEscapePrefixWithKvSep() {
    Map<Character, Character> map = new HashMap<>();
    map.put('\\', '\\');
    map.put('a', 'a');
    new ColumnKeyTranslator('a', '\\', new Escaper('\\', map));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCannotEscapeTagSep() {
    Map<Character, Character> map = new HashMap<>();
    map.put('\\', '\\');
    map.put('b', 'b');
    new ColumnKeyTranslator('a', 'b', new Escaper('\\', map));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCannotEscapeKvSep() {
    Map<Character, Character> map = new HashMap<>();
    map.put('\\', '\\');
    map.put('a', 'a');
    new ColumnKeyTranslator('a', 'b', new Escaper('\\', map));
  }

  private static ColumnKeyTranslator getTranslator() {
    Map<Character, Character> map = new HashMap<>();
    map.put('\\', '\\');
    map.put(',', ',');
    map.put('=', '=');
    return new ColumnKeyTranslator(',', '=', new Escaper('\\', map));
  }

  @Test
  public void testConstructor() {
    getTranslator();
  }

  @Test
  public void toIdentifier() {
    ColumnKeyTranslator translator = getTranslator();

    // When tag is empty
    {
      String path = "path";
      Map<String, String> tags = Collections.emptyMap();
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = translator.translate(columnKey);
      Assert.assertEquals(path, identifier);
    }

    // When tag is not empty
    {
      String path = "path";
      Map<String, String> tags = Collections.singletonMap("tag", "value");
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = translator.translate(columnKey);
      Assert.assertEquals("path,tag=value", identifier);
    }

    // When tag is not empty and has multiple tags
    {
      String path = "path";
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "value1");
      tags.put("tag2", "value2");
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = translator.translate(columnKey);
      Assert.assertEquals("path,tag1=value1,tag2=value2", identifier);
    }

    // When need to escape
    {
      String path = "\\p,a=t\\h,";
      Map<String, String> tags = new HashMap<>();
      tags.put(",,ta,g1,", ",val,ue,,");
      tags.put("=ta=g2==", "==val=ue=");
      tags.put("\\ta\\g3\\\\", "\\\\val\\ue\\");
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = translator.translate(columnKey);
      Assert.assertEquals(
          "\\\\p\\,a\\=t\\\\h\\,,\\,\\,ta\\,g1\\,=\\,val\\,ue\\,\\,,\\=ta\\=g2\\=\\==\\=\\=val\\=ue\\=,\\\\ta\\\\g3\\\\\\\\=\\\\\\\\val\\\\ue\\\\",
          identifier);
    }
  }

  @Test
  public void parse() throws ParseException {
    ColumnKeyTranslator translator = getTranslator();

    // When tag is empty
    {
      String path = "path";
      Map<String, String> tags = Collections.emptyMap();
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey = translator.translate(translator.translate(columnKey));
      Assert.assertEquals(columnKey, parsedColumnKey);
    }

    // When tag is not empty
    {
      String path = "path";
      Map<String, String> tags = Collections.singletonMap("tag", "value");
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey = translator.translate("path,tag=value");
      Assert.assertEquals(columnKey, parsedColumnKey);
    }

    // When tag is not empty and has multiple tags
    {
      String path = "path";
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "value1");
      tags.put("tag2", "value2");
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey = translator.translate("path,tag1=value1,tag2=value2");
      Assert.assertEquals(columnKey, parsedColumnKey);
    }

    // When need to escape
    {
      String path = "\\p,a=t\\h,";
      Map<String, String> tags = new HashMap<>();
      tags.put(",,ta,g1,", ",val,ue,,");
      tags.put("=ta=g2==", "==val=ue=");
      tags.put("\\ta\\g3\\\\", "\\\\val\\ue\\");
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey =
          translator.translate(
              "\\\\p\\,a\\=t\\\\h\\,,\\,\\,ta\\,g1\\,=\\,val\\,ue\\,\\,,\\=ta\\=g2\\=\\==\\=\\=val\\=ue\\=,\\\\ta\\\\g3\\\\\\\\=\\\\\\\\val\\\\ue\\\\");
      Assert.assertEquals(columnKey, parsedColumnKey);
    }
  }

  private static void testWithStringGenerator(Supplier<String> stringSupplier, int times)
      throws ParseException {
    ColumnKeyTranslator translator = getTranslator();
    Random random = new Random();
    for (int i = 0; i < times; i++) {
      String path = stringSupplier.get();
      Map<String, String> tags = new HashMap<>();
      int tagCount = random.nextInt(10);
      for (int j = 0; j < tagCount; j++) {
        String tag = stringSupplier.get();
        String value = stringSupplier.get();
        tags.put(tag, value);
      }
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = translator.translate(columnKey);
      ColumnKey parsedColumnKey = translator.translate(identifier);
      Assert.assertEquals(columnKey, parsedColumnKey);
    }
  }

  @Test
  public void randomColumnKeysAllCodePoint() throws ParseException {
    testWithStringGenerator(() -> RandomStringUtils.random(10), 1000);
  }

  @Test
  public void randomColumnKeysAscii() throws ParseException {
    testWithStringGenerator(() -> RandomStringUtils.randomAscii(10), 2000);
  }
}
