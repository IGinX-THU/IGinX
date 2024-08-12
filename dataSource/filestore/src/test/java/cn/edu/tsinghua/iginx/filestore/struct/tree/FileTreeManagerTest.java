package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem.exec.FileSystemManager;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;

import static org.junit.jupiter.api.Assertions.*;

class FileTreeManagerTest {

  @Test
  public void testPathRoot(){
    for(Path path: FileSystems.getDefault().getRootDirectories()){
      System.out.println(path.getFileName());
    }
    Path test = Paths.get("C:/data/data2");
    System.out.println(test.getParent());
    test.forEach(System.out::println);
  }

}