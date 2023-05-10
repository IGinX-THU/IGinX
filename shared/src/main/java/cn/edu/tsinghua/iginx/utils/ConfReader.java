package cn.edu.tsinghua.iginx.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfReader {

    private static final Logger logger = LoggerFactory.getLogger(ConfReader.class);

    public static String getPropertyVal(String confPath, String property) {
        String val = null;
        try {
            InputStream in = new FileInputStream(confPath);
            Properties properties = new Properties();
            properties.load(in);
            val = properties.getProperty(property);
            if (val == null || val.length() == 0) return null;
        } catch (IOException e) {
            logger.error("get conf {} fail!", confPath);
        }
        return val;
    }
}
