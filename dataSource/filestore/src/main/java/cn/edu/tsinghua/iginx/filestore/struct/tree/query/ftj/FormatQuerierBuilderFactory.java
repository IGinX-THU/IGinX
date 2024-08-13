package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import cn.edu.tsinghua.iginx.filestore.format.FileFormatManager;
import cn.edu.tsinghua.iginx.filestore.format.raw.RawFormat;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder.Factory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nullable;
import java.nio.file.Path;

public class FormatQuerierBuilderFactory implements Factory {

  @Override
  public Builder create(@Nullable String prefix, Path path, FileTreeConfig config) {
    String extension = getExtension(path);
    FileFormat format = FileFormatManager.getInstance().getByExtension(extension, RawFormat.INSTANCE);
    Config configForFormat = config.getFormats().getOrDefault(format.getName(), ConfigFactory.empty());
    return new FormatQuerierBuilder(prefix, path, format, configForFormat);
  }

  @Nullable
  private static String getExtension(Path path) {
    String fileName = path.getFileName().toString();
    int dotIndex = fileName.lastIndexOf('.');
    if (dotIndex == -1) {
      return null;
    }
    return fileName.substring(dotIndex + 1);
  }
}
