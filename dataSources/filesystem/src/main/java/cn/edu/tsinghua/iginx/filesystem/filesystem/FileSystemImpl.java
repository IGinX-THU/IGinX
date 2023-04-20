package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileType;
import cn.edu.tsinghua.iginx.filesystem.query.FSResultTable;
import cn.edu.tsinghua.iginx.filesystem.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/*
 *缓存，索引以及优化策略都在这里执行
 */
public class FileSystemImpl {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemImpl.class);
    IFileOperator fileOperator;
    Charset charset = StandardCharsets.UTF_8;
    String MYWILDCARD = FilePath.MYWILDCARD;

    // set the fileSystem type with constructor
    public FileSystemImpl(/*FileSystemType type*/) {
        fileOperator = new DefaultFileOperator();
        FilePath.setSeparator(System.getProperty("file.separator"));
    }

    //delete it after the UT
    public List<Record> readFile(File file) throws IOException {
        return readFile(file, -1, -1);
    }

    public List<FSResultTable> readFile(File file, Filter filter) throws IOException {
        return readFile(file,null,filter);
    }

    public List<FSResultTable> readFile(File file, TagFilter tagFilter, Filter filter) throws IOException {
        List<FSResultTable> res = new ArrayList<>();

        List<File> files = getFilesWithTagFilter(file,tagFilter);
        if(files == null) {
            return new ArrayList<>();
        }

        long startTime = -1, endTime = -1;
        if(filter!=null) {
            // may fix it 先不支持下推，所以filter中是时间的过滤条件
            AndFilter andFilter = (AndFilter) filter;
            for(Filter f : andFilter.getChildren()) {
                KeyFilter keyFilter = (KeyFilter) f;
                if(keyFilter.getOp().equals(Op.GE)){
                    startTime = keyFilter.getValue();
                } else if (keyFilter.getOp().equals(Op.L)) {
                    endTime = keyFilter.getValue()-1;
                }
            }
        }


        for(File f : files) {
            List<Record> val = new ArrayList<>();
            val = doReadFile(f, startTime, endTime);
            FileMeta fileMeta = null;
            if(FileType.getFileType(f)== FileType.Type.IGINX_FILE) {
                fileMeta= fileOperator.getFileMeta(f);
                res.add(new FSResultTable(f,val,fileMeta.getDataType(),fileMeta.getTag()));
            }else {
                res.add(new FSResultTable(f,val,DataType.BINARY,null));
            }

        }

        return res;
    }

    // read the part of the file
    public List<Record> readFile(File file, long begin, long end) throws IOException {
        return doReadFile(file, begin, end);
    }

    private List<Record> doReadFile(File file, long begin, long end) throws IOException {
        List<Record> res = new ArrayList<>();
        switch (FileType.getFileType(file)) {
            case DIR:
                res = fileOperator.dirReader(file);
                break;
            case IGINX_FILE:
                res = fileOperator.iginxFileReaderByKey(file, begin, end, charset);
                break;
            case NORMAL_FILE:
                res = fileOperator.normalFileReader(file, begin, end, charset);
                break;
            default:
                res = fileOperator.normalFileReader(file, begin, end, charset);
        }
        return res;
    }

    public boolean mkDir(File file) {
        return fileOperator.mkdir(file);
    }

    public Exception writeFile(File file, List<Record> value, boolean append) throws IOException {
        return doWriteFile(file, value, append);
    }

    public Exception writeFile(File file, List<Record> value, Map<String, String> tag, boolean append) throws IOException {
        File tmpFile;
        tmpFile = getFileWithTag(file, tag);
        if(tmpFile==null) {
            file = determineFileId(file,tag);
            file = createIginxFile(file, value.get(0).getDataType(), tag);
        }else {
            file = tmpFile;
        }

        return doWriteFile(file, value, append);
    }

    public Exception writeFiles(List<File> files, List<List<Record>> values, List<Boolean> appends) throws IOException {
        return writeFiles(files, values,null,appends);
    }

    // write multi file
    public Exception writeFiles(List<File> files, List<List<Record>> values, List<Map<String, String>> tagList, List<Boolean> appends) throws IOException {
        for (int i = 0; i < files.size(); i++) {
            writeFile(files.get(i), values.get(i), tagList.get(i), appends.get(i));
        }
        return null;
    }

    public Exception deleteFile(File file) throws IOException {
        return deleteFiles(Collections.singletonList(file),null);
    }

    /**
     * 删除文件或目录
     *
     * @param files 要删除的文件或目录列表
     * @throws Exception 如果删除操作失败则抛出异常
     */
    public Exception deleteFiles(List<File> files, TagFilter filter) throws IOException {
        List<File> fileList = new ArrayList<>();
        for (File file : files) {
            List<File> tmp = getFilesWithTagFilter(file,filter);
            if(tmp==null) continue;
            fileList.addAll(tmp);
        }
        for (File file : fileList) {
             if (!fileOperator.delete(file)) {
                    return new IOException("Failed to delete file: "+file.getAbsolutePath());
                }
        }
        return null;
    }

    public Exception trimFileContent(File file, long begin, long end) throws IOException {
        return trimFilesContent(Collections.singletonList(file), null,begin, end);
    }

    public Exception trimFileContent(File file,  TagFilter tagFilter,long begin, long end) throws IOException {
        return trimFilesContent(Collections.singletonList(file), tagFilter,begin, end);
    }

    public Exception trimFilesContent(List<File> files, TagFilter tagFilter,long begin, long end) throws IOException {
        for(File file : files) {
            List<File> fileList = getFilesWithTagFilter(file,tagFilter);
            if(fileList==null) {
                logger.warn("cant trim the file that not exist!");
                continue;
            }
            for(File f : fileList) {
                fileOperator.fileTrimmer(f, begin, end);
            }
        }
        return null;
    }


    private File createIginxFile(File file, DataType dataType, Map<String, String> tag) throws IOException {
        return fileOperator.create(file, new FileMeta(dataType,tag));
    }

    private boolean ifFileExists(File file, TagFilter tagFilter) throws IOException {
        if(getFilesWithTagFilter(file,tagFilter) != null) {
            return true;
        }
        return false;
    }

    private boolean ifFileExists(File file) {
        return fileOperator.ifFileExists(file);
    }

    private int getFileID(File file, Map<String, String> tag) throws IOException {
        Path path = Paths.get(file.getPath());
        List<File> files = getAssociatedFiles(file);

        List<Integer> nums = new ArrayList<>();
        nums.add(0);
        if(files==null) return -1;
        for (File f : files) {
            String name = f.getName();
            int idx = name.lastIndexOf(".iginx");
            String numStr = name.substring(idx + 6);
            if(numStr.isEmpty()) continue;
            nums.add(Integer.parseInt(numStr));
        }

        return Collections.max(nums);
    }

    private File determineFileId(File file, Map<String, String> tag) throws IOException {
        int id = getFileID(file, tag);
        if(id==-1) id=0;
        else id+=1;
        String path = file.getAbsolutePath()+id;
        return new File(path);
    }

    private Exception doWriteFile(File file, List<Record> value, boolean append) throws IOException {
        Exception res =null;
        byte[] bytes;

        switch (FileType.getFileType(file)) {
            case DIR:
                if(!mkDir(file)) {
                    logger.error("create dir fail!");
                    throw new IOException("create dir fail!");
                }
                break;
            case IGINX_FILE:
                res = fileOperator.iginxFileWriter(file, value);
                break;
            case NORMAL_FILE://may fix it ,因为写入只能对于iginx文件
//                bytes = makeValueToBytes(value);
//                res = fileOperator.TextFileWriter(file, bytes, append);
                break;
            default:
                res = fileOperator.iginxFileWriter(file, value);
        }
        return res;
    }

    private byte[] makeValueToBytes(List<Record> value) throws IOException {
        List<byte[]> byteList = new ArrayList<>();
        switch (value.get(0).getDataType()) {
            case BINARY:
            default:
                for (Record val : value) {
                    byteList.add((byte[]) val.getRawData());
                }
                break;
        }
        return mergeByteArrays(byteList);
    }

    public byte[] mergeByteArrays(List<byte[]> arrays) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            outputStream.write(array);
        }
        return outputStream.toByteArray();
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }



    //
    public List<File> getAssociatedFiles(File file) {
        List<File> fileList,filess= null;
        Stack<File> S =new Stack<>();
        Set<File> res = new HashSet<>();
        File root = null;
        String prefix = null;
        // select * from *
        if(file.getParentFile().getName().equals(MYWILDCARD) && file.getName().contains(MYWILDCARD)) {
            root = file.getParentFile().getParentFile();// storage unit file
        } else if (file.getParentFile().getName().equals(MYWILDCARD) && !file.getName().contains(MYWILDCARD)) {
            File tmp = file.getParentFile();
            while(tmp.getName().equals(MYWILDCARD)) {
                tmp=tmp.getParentFile();
            }
            root=tmp;
            prefix=file.getName();
        } else if(file.getName().contains(MYWILDCARD)) {

            root = file.getParentFile();
        }else if(file.isDirectory()) {
            root = file;
        } else{
            root = file.getParentFile();
            prefix= file.getName();
        }
        boolean flag = false;
        S.push(root);
        while(!S.empty()){
            File tmp = S.pop();
            if(tmp.isDirectory()) {
                List<File> files = fileOperator.listFiles(tmp,prefix),dirlist=fileOperator.listFiles(tmp);
                if(files!=null) {
                    for(File f : files) S.push(f);
                }
                if(dirlist!=null) {
                    for(File f : dirlist) {
                        if(f.isDirectory()) S.push(f);
                    }
                }
            }
            if(flag) res.add(tmp);
            flag=true;
        }
        fileList = new ArrayList<>(res);
        return fileList.size()==0?null:fileList;
    }

    /**
     根据提供的 tags 集合查找同名 / 近名的 .iginx 文件,其元数据 tags 与提供的集合相等。
     若找到,返回该文件,否则返回 null。
     @param tags 用于匹配的 tags 集合
     @param file 用于查找相同名或近名的父级目录
     @return 元数据与 tags 相等的 .iginx 文件,否则返回 null
     @throws IOException 任何查找或读写操作导致的 IOException 将被传播
     */
    private File getFileWithTag( File file,Map<String, String> tags) throws IOException {
        List<File> res = getAssociatedFiles(file);
        if(res==null) return null;
        for(File fi : res) {
            if(fi.isDirectory()) continue;
            FileMeta fileMeta = fileOperator.getFileMeta(fi);
            if(Objects.equals(tags,fileMeta.getTag())){
                return fi;
            }
        }
        return null;
    }

    private List<File> getFilesWithTagFilter(File file,TagFilter tagFilter) throws IOException {
        List<File> files = getAssociatedFiles(file), res = new ArrayList<>();
        if(files==null) return files;
        for(File fi : files) {
            if(fi.isDirectory() || !fileOperator.ifFileExists(fi)) continue;
            FileMeta fileMeta = null;
            if(FileType.getFileType(fi)== FileType.Type.IGINX_FILE)
                fileMeta = fileOperator.getFileMeta(fi);
            if(tagFilter ==null || TagKVUtils.match(fileMeta.getTag(), tagFilter)) {
                res.add(fi);
            }
        }
        return res.size()==0?null:res;
    }

    /**
     根据提供的 tags 集合查找同名 / 近名的 .iginx 文件,其元数据包含 tags 中的所有 key-value 对。
     若找到,返回包含这些文件的集合,否则返回 null。
     @param tags 用于匹配的 tags 集合
     @param file 用于查找相同名或近名的父级目录
     @return 包含 tags 中所有 key-value 对的 .iginx 文件集合,否则返回 null
     @throws IOException 任何查找或读写操作导致的 IOException 将被传播
     */
    private List<File> getFilesContainTag(File file,Map<String, String> tags) throws IOException {
        List<File> res = new ArrayList<>();
        List<File> files = getAssociatedFiles(file);

        if(files==null) return null;
        for(File f : files) {
            if(f.isDirectory()) continue;
            FileMeta fileMeta = fileOperator.getFileMeta(f);
            if(fileMeta.ifContainTag(tags)){
                res.add(f);
            }
        }
        return res.size()==0?null:res;
    }

    public List<Pair<File,FileMeta>> getAllIginXFiles(File dir) {
        List<Pair<File,FileMeta>> res = new ArrayList<>();
        Stack<File> stack = new Stack<>();
        stack.push(dir);
        while (!stack.isEmpty()) {
            File current = stack.pop();
            List<File> fileList = null;
            if (current.isDirectory())
                fileList = fileOperator.listFiles(current);
            else if(FileType.getFileType(current)== FileType.Type.IGINX_FILE){
                try {
                    res.add(new Pair<>(current,fileOperator.getFileMeta(current)));
                }catch (IOException e){
                    logger.error(e.getMessage());
                }
            }
            if (fileList != null) {
                for (File file : fileList) {
                    stack.push(file);
                }
            }
        }
        return res;
    }

    // 返回最大最小的文件，文件可能是目录
    public List<File> getBoundaryFiles(File dir) {
        File minFile = getMinFile(dir);
        File maxFile = getMaxFile(dir);

        List<File> res = new ArrayList<>();
        res.add(minFile);

        File lastFile = null;
        while(maxFile.isDirectory()) {
            File[] files = maxFile.listFiles();
            if(files!=null)
                maxFile = files[files.length - 1];
            if(lastFile!=null && fileOperator.ifFilesEqual(lastFile,maxFile)) {
                break;
            }
            lastFile = maxFile;
        }

        res.add(maxFile);
        return res;
    }

    File getMinFile(File dir) {
        File[] files = dir.listFiles();
        Arrays.sort(files);
        return files[0];
    }

    File getMaxFile(File dir) {
        File[] files = dir.listFiles();
        if(files.length==0){
            return dir;
        }
        Arrays.sort(files);
        File maxFile = files[files.length - 1];
        return maxFile;
    }

    public List<Long> getBoundaryTime(File dir) {
        List<Long> res =new ArrayList<>();
        List<File> files = getAssociatedFiles(dir);
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (File f : files) {
            long size = f.length();
            min = Math.min(min, size);
            max = Math.max(max, size);
        }

        res.add(min);
        res.add(max);
        return res;
    }

    public Date getCreationTime(File file){
        return fileOperator.getCreationTime(file);
    }

    private long getFileSize(File file) {
        try {
            if(file.exists()) {
                if(file.isFile()) {
                    return fileOperator.length(file);
                } else {
                    return 0;
                }
            }
        }catch (IOException e){
            logger.error(e.getMessage());
        }

        return 0;
    }
}
