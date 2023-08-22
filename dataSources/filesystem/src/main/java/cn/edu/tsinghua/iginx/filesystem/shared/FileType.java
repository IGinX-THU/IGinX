package cn.edu.tsinghua.iginx.filesystem.shared;

import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.MAGIC_NUMBER;

import cn.edu.tsinghua.iginx.filesystem.file.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public enum FileType {
  IGINX_FILE,
  NORMAL_FILE,
  UNKNOWN_FILE,
  DIR;
}
