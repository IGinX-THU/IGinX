package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public interface IFileOperator {
  List<Record> readDir(File file) throws IOException;
  // read the all the file
  List<Record> readNormalFile(File file, long begin, long end, Charset charset) throws IOException;

  // read the file by key [begin, end]
  List<Record> readIGinXFileByKey(File file, long begin, long end, Charset charset)
      throws IOException;

  Exception writeIGinXFile(File file, List<Record> valList) throws IOException;

  Exception trimFile(File file, long begin, long end) throws IOException;

  boolean delete(File file);

  File create(File file, FileMeta fileMeta) throws IOException;

  boolean mkdir(File file);

  boolean isDirectory(File file);

  FileMeta getFileMeta(File file) throws IOException;

  Boolean ifFileExists(File file);

  List<File> listFiles(File file);

  List<File> listFiles(File file, String prefix);

  long length(File file) throws IOException;

  boolean ifFilesEqual(File... file);
}
