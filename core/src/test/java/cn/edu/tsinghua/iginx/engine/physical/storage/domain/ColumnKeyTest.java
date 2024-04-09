package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.Assert;
import org.junit.Test;

public class ColumnKeyTest {

  @Test
  public void toIdentifier() {
    // When tag is null
    try {
      String path = "path";
      Map<String, String> tags = null;
      new ColumnKey(path, tags);
      Assert.fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }

    // When path is null
    try {
      String path = null;
      Map<String, String> tags = Collections.emptyMap();
      new ColumnKey(path, tags);
      Assert.fail("Should throw NullPointerException");
    } catch (NullPointerException ignored) {
    }

    // When tag is empty
    {
      String path = "path";
      Map<String, String> tags = Collections.emptyMap();
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = columnKey.toIdentifier();
      Assert.assertEquals(path, identifier);
    }

    // When tag is not empty
    {
      String path = "path";
      Map<String, String> tags = Collections.singletonMap("tag", "value");
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = columnKey.toIdentifier();
      Assert.assertEquals("path,tag=value", identifier);
    }

    // When tag is not empty and has multiple tags
    {
      String path = "path";
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "value1");
      tags.put("tag2", "value2");
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = columnKey.toIdentifier();
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
      String identifier = columnKey.toIdentifier();
      Assert.assertEquals(
          "\\\\p\\,a\\=t\\\\h\\,,\\,\\,ta\\,g1\\,=\\,val\\,ue\\,\\,,\\=ta\\=g2\\=\\==\\=\\=val\\=ue\\=,\\\\ta\\\\g3\\\\\\\\=\\\\\\\\val\\\\ue\\\\",
          identifier);
    }
  }

  @Test
  public void parse() throws ParseException {
    // When tag is empty
    {
      String path = "path";
      Map<String, String> tags = Collections.emptyMap();
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey = ColumnKey.parseIdentifier(path);
      Assert.assertEquals(columnKey, parsedColumnKey);
    }

    // When tag is not empty
    {
      String path = "path";
      Map<String, String> tags = Collections.singletonMap("tag", "value");
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey = ColumnKey.parseIdentifier("path,tag=value");
      Assert.assertEquals(columnKey, parsedColumnKey);
    }

    // When tag is not empty and has multiple tags
    {
      String path = "path";
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "value1");
      tags.put("tag2", "value2");
      ColumnKey columnKey = new ColumnKey(path, tags);
      ColumnKey parsedColumnKey = ColumnKey.parseIdentifier("path,tag1=value1,tag2=value2");
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
          ColumnKey.parseIdentifier(
              "\\\\p\\,a\\=t\\\\h\\,,\\,\\,ta\\,g1\\,=\\,val\\,ue\\,\\,,\\=ta\\=g2\\=\\==\\=\\=val\\=ue\\=,\\\\ta\\\\g3\\\\\\\\=\\\\\\\\val\\\\ue\\\\");
      Assert.assertEquals(columnKey, parsedColumnKey);
    }
  }

  private static void testWithStringGenerator(
      RandomStringGenerator randomStringGenerator, int times) throws ParseException {
    Random random = new Random();
    for (int i = 0; i < times; i++) {
      String path = randomStringGenerator.generate(0, 20);
      Map<String, String> tags = new HashMap<>();
      int tagCount = random.nextInt(10);
      for (int j = 0; j < tagCount; j++) {
        String tag = randomStringGenerator.generate(0, 10);
        String value = randomStringGenerator.generate(0, 10);
        tags.put(tag, value);
      }
      ColumnKey columnKey = new ColumnKey(path, tags);
      String identifier = columnKey.toIdentifier();
      ColumnKey parsedColumnKey = ColumnKey.parseIdentifier(identifier);
      Assert.assertEquals(columnKey, parsedColumnKey);
      System.out.println("Test " + columnKey);
    }
  }

  @Test
  public void randomColumnKeysAllCodePoint() throws ParseException {
    RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder().build();
    testWithStringGenerator(randomStringGenerator, 10000);
  }

  @Test
  public void randomColumnKeysAscii() throws ParseException {
    RandomStringGenerator randomStringGenerator =
        new RandomStringGenerator.Builder().withinRange(0, 127).build();
    testWithStringGenerator(randomStringGenerator, 100000);
  }
}
