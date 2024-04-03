package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.auth.FilePermissionManager;
import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.auth.entity.Module;
import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileAppendWriter extends ExportWriter {

  private final String fileName;

  private boolean hasWriteHeader;

  private static final Logger logger = LoggerFactory.getLogger(FileAppendWriter.class);

  public FileAppendWriter(String fileName) {
    this.fileName = fileName;
    this.hasWriteHeader = false;
    File file = new File(fileName);
    createFileIfNotExist(file);
  }

  @Override
  public void write(BatchData batchData) {
    if (!hasWriteHeader) {
      Header header = batchData.getHeader();

      List<String> headerList = new ArrayList<>();
      if (header.hasKey()) {
        headerList.add(GlobalConstant.KEY_NAME);
      }
      header.getFields().forEach(field -> headerList.add(field.getFullName()));
      writeFile(fileName, String.join(",", headerList) + "\n");
      hasWriteHeader = true;
    }
    for (Row row : batchData.getRowList()) {
      writeFile(fileName, row.toCSVTypeString() + "\n");
    }
  }

  private void createFileIfNotExist(File file) {
    Predicate<Path> pathChecker =
        FilePermissionManager.getInstance()
            .getChecker(null, Module.TRANSFORMER, FileAccessType.WRITE);
    if (!pathChecker.test(file.toPath())) {
      throw new SecurityException(
          ("transformer has no permission to write file: " + file.getAbsolutePath()));
    }
    if (!file.exists()) {
      logger.info("File not exists, create it...");
      // get and create parent dir
      if (!file.getParentFile().exists()) {
        System.out.println("Parent dir not exists, create it...");
        file.getParentFile().mkdirs();
      }
      try {
        // create file
        file.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void writeFile(String fileName, String content) {
    try {
      File file = new File(fileName);

      try (FileWriter writer = new FileWriter(file, true);
          BufferedWriter out = new BufferedWriter(writer)) {
        out.write(content);
        out.flush();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
