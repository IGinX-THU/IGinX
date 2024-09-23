/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.tree;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.format.raw.RawFormat;
import cn.edu.tsinghua.iginx.filesystem.format.raw.RawReaderConfig;
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
