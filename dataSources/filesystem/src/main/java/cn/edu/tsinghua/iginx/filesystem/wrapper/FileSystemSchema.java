package cn.edu.tsinghua.iginx.filesystem.wrapper;

import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;

import java.util.Map;

/*
 * 主要是为了处理 文件名 <==> 存储到文件系统中的文件名 间的映射关系
 * 主要处理 tagkv 以及 序列名到文件名 的映射关系
 */
public class FileSystemSchema {
    // 还没有处理tagkv以及文件名的对应关系， fix it
    public static String getRealFilePath(FilePath path, Map<String, String> tags) {
        return path.getFilePath();
    }
}
