package cn.edu.tsinghua.iginx.filestore.struct.tree;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.format.raw.RawFormat;
import cn.edu.tsinghua.iginx.filestore.format.raw.RawReaderConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FileTreeConfigTest {

  @Test
  public void testEmptyFormats() {
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(ConfigFactory.empty());

    assertEquals(Collections.emptyMap(), fileTreeConfig.getFormats());
  }

  @Test
  public void testFormats() {
    Map<String, Object> rawConfigMap = new HashMap<>();
    rawConfigMap.put(
        String.join(
            ".", FileTreeConfig.Fields.formats, RawFormat.NAME, RawReaderConfig.Fields.pageSize),
        4096);

    Config rawConfig = ConfigFactory.parseMap(rawConfigMap);

    FileTreeConfig fileTreeConfig = FileTreeConfig.of(rawConfig);

    Map<String, Config> formats = fileTreeConfig.getFormats();
    Map<String, Config> expectedFormats =
        Collections.singletonMap(
            RawFormat.NAME,
            ConfigFactory.parseMap(
                Collections.singletonMap(RawReaderConfig.Fields.pageSize, 4096)));
    assertEquals(expectedFormats, formats);
  }

  @Test
  public void testIgnoreInvalidFormats() {
    Map<String, Object> rawConfigMap = new HashMap<>();
    rawConfigMap.put(String.join(".", FileTreeConfig.Fields.formats), 4096);

    Config rawConfig = ConfigFactory.parseMap(rawConfigMap);
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(rawConfig);

    assertEquals(Collections.emptyMap(), fileTreeConfig.getFormats());
  }

  @Test
  public void testIgnoreInvalidFormatsField() {
    Map<String, Object> rawConfigMap = new HashMap<>();
    rawConfigMap.put(String.join(".", FileTreeConfig.Fields.formats, RawFormat.NAME), 4096);

    Config rawConfig = ConfigFactory.parseMap(rawConfigMap);
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(rawConfig);

    assertEquals(Collections.emptyMap(), fileTreeConfig.getFormats());
  }

  @Test
  public void testIgnoreInvalidDot() {
    Map<String, Object> rawConfigMap = new HashMap<>();
    rawConfigMap.put(String.join(".", FileTreeConfig.Fields.dot), ".");

    Config rawConfig = ConfigFactory.parseMap(rawConfigMap);
    FileTreeConfig fileTreeConfig = FileTreeConfig.of(rawConfig);

    {
      List<AbstractConfig.ValidationProblem> problemList = fileTreeConfig.validate();
      assertEquals(1, problemList.size());
      assertEquals("dot:'dot cannot be '.''", problemList.get(0).toString());
    }

    {
      fileTreeConfig.setDot(null);
      List<AbstractConfig.ValidationProblem> problemList = fileTreeConfig.validate();
      assertEquals(1, problemList.size());
      assertEquals("dot:'missing field'", problemList.get(0).toString());
    }
  }
}
