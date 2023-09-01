package cn.edu.tsinghua.iginx.engine.shared.file.write;

import cn.edu.tsinghua.iginx.engine.shared.file.FileType;

public class ExportByteStream implements ExportFile {

  private final String dir;

  public ExportByteStream(String dir) {
    this.dir = dir;
  }

  public String getDir() {
    return dir;
  }

  @Override
  public FileType getType() {
    return FileType.BYTE_STREAM;
  }
}
