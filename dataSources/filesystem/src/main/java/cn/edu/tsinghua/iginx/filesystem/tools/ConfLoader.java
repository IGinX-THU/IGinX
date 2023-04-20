package cn.edu.tsinghua.iginx.filesystem.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfLoader.class);
    static final private String confPath = "D:\\Desktop\\Study\\THU\\code\\MY\\IGinX\\dataSources\\filesystem\\src\\main\\java\\cn\\edu\\tsinghua\\iginx\\filesystem\\conf\\config.properties";
    static final private String ROOT = "root";
    public static String getRootPath() {
        String path = null;
        try {
            InputStream in = new FileInputStream(confPath);
            Properties properties = new Properties();
            properties.load(in);
            path= properties.getProperty(ROOT);
            if(path ==null||path.length()==0)return null;
        }catch (IOException e){
            logger.error("get conf {} fail!",confPath);
        }
        return path;
    }

}

