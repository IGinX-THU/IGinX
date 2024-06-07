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
