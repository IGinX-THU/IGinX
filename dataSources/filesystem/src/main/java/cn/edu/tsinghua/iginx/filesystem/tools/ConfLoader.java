package cn.edu.tsinghua.iginx.filesystem.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import cn.edu.tsinghua.iginx.utils.ConfReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfLoader.class);
    private static final String confPath =
            "dataSources/filesystem/src/main/resources/conf/config.properties";
    private static final String ROOT = "root";
    private static final String isLocal = "isLocalFileSystemStorage";
    public String ROOTPATH = null;

    public static String getRootPath() {
    return ConfReader.getPropertyVal(confPath,ROOT);
    }

    public static File getRootFile() {
        String root = getRootPath();
        return new File(root);
    }

    public static boolean ifLocalFileSystem() {
       return Boolean.parseBoolean( ConfReader.getPropertyVal(confPath,isLocal));
    }
}
