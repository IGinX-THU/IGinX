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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class YAMLReader {

  private final String path;

  private final Yaml yaml;

  private final File file;

  private static final Logger LOGGER = LoggerFactory.getLogger(YAMLReader.class);

  public YAMLReader(String path) throws FileNotFoundException {
    this.path = path;
    this.yaml = new Yaml(new Constructor(JobFromYAML.class, new LoaderOptions()));
    this.file = new File(path);
    if (!file.exists()) {
      throw new FileNotFoundException(file.getAbsolutePath());
    }
  }

  public String normalize(String conf) {
    String taskType = "(?i)taskType";
    String dataFlowType = "(?i)dataFlowType";
    String timeout = "(?i)timeout";
    String pyTaskName = "(?i)pyTaskName";
    String sqlList = "(?i)sqlList";

    conf = conf.replaceAll(taskType, "taskType");
    conf = conf.replaceAll(dataFlowType, "dataFlowType");
    conf = conf.replaceAll(timeout, "timeout");
    conf = conf.replaceAll(pyTaskName, "pyTaskName");
    conf = conf.replaceAll(sqlList, "sqlList");

    String exportType = "(?i)exportType";
    String exportFile = "(?i)exportFile";
    String exportNameList = "(?i)exportNameList";
    String schedule = "(?i)schedule";

    conf = conf.replaceAll(exportType, "exportType");
    conf = conf.replaceAll(exportFile, "exportFile");
    conf = conf.replaceAll(exportNameList, "exportNameList");
    conf = conf.replaceAll(schedule, "schedule");

    return conf;
  }

  public String convertToString(String filePath) {
    String conf = null;
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(filePath));
      conf = IOUtils.toString(in, String.valueOf(StandardCharsets.UTF_8));
      conf = normalize(conf);
    } catch (IOException e) {
      LOGGER.error("Fail to find file, path={}", filePath);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOGGER.error("Fail to close the file, path={}", filePath);
      }
    }
    return conf;
  }

  public JobFromYAML getJobFromYAML() {
    String yamlFile = convertToString(path);
    InputStream result = new ByteArrayInputStream(yamlFile.getBytes(StandardCharsets.UTF_8));
    return yaml.load(result);
  }
}
