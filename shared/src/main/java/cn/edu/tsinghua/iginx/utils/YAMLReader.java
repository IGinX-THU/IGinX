/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;

public class YAMLReader {

  private final String path;

  private final Yaml yaml;

  private final File file;

  private static final Logger LOGGER = LoggerFactory.getLogger(YAMLReader.class);

  public YAMLReader(String path) throws FileNotFoundException {
    this.path = path;
    this.yaml = new Yaml(new YAMLConstructor(JobFromYAML.class, new LoaderOptions()));
    this.file = new File(path);
    if (!file.exists()) {
      throw new FileNotFoundException(file.getAbsolutePath());
    }
  }

  public JobFromYAML getJobFromYAML() {
    try (InputStream in =
        new ByteArrayInputStream(
            new String(java.nio.file.Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8)
                .getBytes(StandardCharsets.UTF_8)); ) {
      return yaml.loadAs(in, JobFromYAML.class);
    } catch (FileNotFoundException e) {
      LOGGER.error("Fail to find file, path={}", path, e);
    } catch (IOException e) {
      LOGGER.error("Fail to read the yaml file, path={}", path, e);
    }
    return null;
  }

  public static class YAMLConstructor extends Constructor {
    public YAMLConstructor(Class<?> root, LoaderOptions options) {
      super(root, options);
    }

    @Override
    protected Map<Object, Object> constructMapping(MappingNode node) {
      List<NodeTuple> tuples = node.getValue();
      List<NodeTuple> normalizedTuples = new ArrayList<>();

      for (NodeTuple tuple : tuples) {
        Node keyNode = tuple.getKeyNode();
        Node valueNode = tuple.getValueNode();

        if (keyNode instanceof ScalarNode) {
          ScalarNode scalarNode = (ScalarNode) keyNode;
          String originalKey = scalarNode.getValue();
          String normalizedKey = normalizeKey(originalKey);

          if (!normalizedKey.equals(originalKey)) {
            // key 是final的，所以需要复制
            ScalarNode newKeyNode =
                new ScalarNode(
                    scalarNode.getTag(),
                    normalizedKey,
                    scalarNode.getStartMark(),
                    scalarNode.getEndMark(),
                    scalarNode.getScalarStyle());
            normalizedTuples.add(new NodeTuple(newKeyNode, valueNode));
          } else {
            normalizedTuples.add(tuple);
          }
        } else {
          normalizedTuples.add(tuple);
        }
      }
      node.setValue(normalizedTuples);
      return super.constructMapping(node);
    }

    private String normalizeKey(String key) {
      String lower = key.toLowerCase(Locale.ROOT);

      switch (lower) {
        case "tasktype":
          return "taskType";
        case "dataflowtype":
          return "dataFlowType";
        case "timeout":
          return "timeout";
        case "pytaskname":
          return "pyTaskName";
        case "outputprefix":
          return "outputPrefix";
        case "sqllist":
          return "sqlList";
        case "exporttype":
          return "exportType";
        case "exportfile":
          return "exportFile";
        case "exportnamelist":
          return "exportNameList";
        case "schedule":
          return "schedule";
      }

      return key;
    }
  }
}
