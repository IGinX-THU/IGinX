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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

public class YAMLWriter {
  private final Yaml yaml;

  private static final Logger LOGGER = LoggerFactory.getLogger(YAMLWriter.class);

  private static final DumperOptions options = new DumperOptions();

  public YAMLWriter() {
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    options.setIndent(2);
    options.setExplicitStart(true);
    LoaderOptions loaderOptions = new LoaderOptions();
    Representer representer = new Representer(options);
    representer.addClassTag(JobFromYAML.class, Tag.MAP);
    representer.setDefaultScalarStyle(DumperOptions.ScalarStyle.DOUBLE_QUOTED);

    this.yaml = new Yaml(new Constructor(JobFromYAML.class, loaderOptions), representer, options);
  }

  /**
   * write job information into given yaml file.
   *
   * @param file target file to write
   * @param job job to dump
   */
  public void writeJobIntoYAML(File file, JobFromYAML job) throws IOException {
    assert file.exists() && file.isFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
      yaml.dump(job, bw);
    }
  }
}
