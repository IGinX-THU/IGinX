package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryDataGeneratorTest extends BaseHistoryDataGenerator {
    private static final Logger logger =
            LoggerFactory.getLogger(FileSystemHistoryDataGeneratorTest.class);
    private String root = "../dataSources/filesystem/src/test/java/cn/edu/tsinghua/iginx/storage/";

    @Test
    public void writeHistoryData() throws Exception {
        File file1 = new File(root + "file/cpu_usage");
        writeToFile(file1, "123");

        File file2 = new File(root + "file/engine");
        writeToFile(file2, "456");

        File file3 = new File(root + "file/status");
        writeToFile(file3, "789");
    }

    public void writeToFile(File file, String str) throws Exception {
        try {
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                Files.createFile(Paths.get(file.getAbsolutePath()));
            }
            FileWriter writer = new FileWriter(file);
            writer.write(str);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("系统找不到指定的路径: " + file.getAbsolutePath());
        }
    }

    @Override
    public void writeHistoryDataToB() throws Exception {
        //        session.executeNonQueryStatement(
        //            "INSERT INTO root.ln.wf03.wt01(timestamp,status) values(77,true);");
        //        session.executeNonQueryStatement(
        //            "INSERT INTO root.ln.wf03.wt01(timestamp,status,temperature)
        // values(200,false,77.71);");
        File file1 = new File(root + "ln/wf03/wt01/status.iginx0");
        FileMeta fileMeta1 = new FileMeta(DataType.BOOLEAN, null);
        writeData(
                file1,
                fileMeta1,
                new ArrayList<Record>() {
                    {
                        add(new Record(77, true));
                        add(new Record(200, false));
                    }
                });

        File file2 = new File(root + "ln/wf03/wt01/temperature.iginx0");
        FileMeta fileMeta2 = new FileMeta(DataType.DOUBLE, null);
        writeData(
                file2,
                fileMeta2,
                new ArrayList<Record>() {
                    {
                        add(new Record(200, 77.71));
                    }
                });
    }

    public void writeData(File file, FileMeta fileMeta, List<Record> val) throws Exception {
        IFileOperator fileOperator = new DefaultFileOperator();
        if (fileOperator.ifFileExists(file)) {
            fileOperator.create(file, fileMeta);
        }
        fileOperator.writeIGinXFile(file, val);
    }

    @Override
    public void writeHistoryDataToA() throws Exception {
        File file1 = new File(root + "ln/wf01/wt01/status.iginx0");
        FileMeta fileMeta1 = new FileMeta(DataType.BOOLEAN, null);
        writeData(
                file1,
                fileMeta1,
                new ArrayList<Record>() {
                    {
                        add(new Record(100, true));
                        add(new Record(200, false));
                    }
                });

        File file2 = new File(root + "ln/wf01/wt01/temperature.iginx0");
        FileMeta fileMeta2 = new FileMeta(DataType.DOUBLE, null);
        writeData(
                file2,
                fileMeta2,
                new ArrayList<Record>() {
                    {
                        add(new Record(200, 20.71));
                    }
                });
    }

    @Override
    public void clearData() {
        deleteDirectory(root);
    }

    public void deleteDirectory(String path) {
        File directory = new File(path);

        // 如果目录不存在,什么也不做
        if (!directory.exists()) return;

        for (File file : directory.listFiles()) {
            // 如果是文件,删除它
            if (file.isFile()) {
                file.delete();
            } else if (file.isDirectory()) {
                // 如果是目录,先删除里面所有的内容
                deleteDirectory(file.getPath());
                // 再删除外层目录
                file.delete();
            }
        }
        directory.delete();
    }
}
