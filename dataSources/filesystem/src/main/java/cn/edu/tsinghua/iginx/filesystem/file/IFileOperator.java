package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public interface IFileOperator {

  // read normal file by [startKey, endKey)
  List<Record> readNormalFile(File file, long startKey, long endKey, Charset charset)
      throws IOException;

  // read IGinX file by [startKey, endKey)
  List<Record> readIginxFile(File file, long startKey, long endKey, Charset charset)
      throws IOException;

  Exception writeIginxFile(File file, List<Record> valList) throws IOException;

  File create(File file, FileMeta fileMeta) throws IOException;

  boolean delete(File file);

  Exception trimFile(File file, long begin, long end);

  FileMeta getFileMeta(File file) throws IOException;

  List<File> listFiles(File file);

  List<File> listFiles(File file, String prefix);

  long length(File file) throws IOException;

  boolean ifFilesEqual(File... file);
}
