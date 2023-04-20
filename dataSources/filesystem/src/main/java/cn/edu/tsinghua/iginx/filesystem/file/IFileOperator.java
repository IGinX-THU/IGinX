package cn.edu.tsinghua.iginx.filesystem.file;

import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;

public interface IFileOperator {
    List<Record> dirReader(File file) throws IOException;
    // read the all the file
    List<Record> normalFileReader(File file, long begin, long end, Charset charset) throws IOException;

    // read the file by lines [begin, end]
//    List<byte[]> TextFileReaderByLine(File file, long begin, long end, Charset charset) throws IOException;

    // read the file by key [begin, end]
    List<Record> iginxFileReaderByKey(File file, long begin, long end, Charset charset) throws IOException;

    // read the byte range [begin, end] of the file
//    List<Record> TextFileReaderByByteSeek(File file, long begin, long end) throws IOException;

//    Exception ByteFileWriter(File file, byte[] bytes, boolean append);

//    Exception TextFileWriter(File file, byte[] bytes, boolean append) throws IOException;

    Exception iginxFileWriter(File file, List<Record> valList) throws IOException;

    Exception fileTrimmer(File file, long begin, long end) throws IOException;

    boolean delete(File file);

    File create(File file, FileMeta fileMeta) throws IOException;

    public boolean mkdir(File file);

    public boolean isDirectory(File file);

    FileMeta getFileMeta(File file) throws IOException;

    Boolean ifFileExists(File file);

    public List<File> listFiles(File file);

    public List<File> listFiles(File file, String prefix);

    public Date getCreationTime(File file);

    long length(File file) throws IOException;

    boolean ifFilesEqual(File... file);
}
