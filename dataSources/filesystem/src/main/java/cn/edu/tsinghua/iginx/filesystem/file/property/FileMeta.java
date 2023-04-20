package cn.edu.tsinghua.iginx.filesystem.file.property;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class FileMeta {
    // the number of the max meta info
    public static long IGINXFILEVALINDEX = 10L;
    // is Dir line number
    final public static int ISDIRINDEX = 1;
    // data type line number
    final public static int DATATYPEINDEX = 2;
    // tagkv line number
    final public static int TAGKVINDEX = 3;
    private DataType dataType;
    private Map<String, String> tag;
    private boolean isDir =false;
    private Date createTime;

    public FileMeta() {
        this.dataType = null;
        this.tag = null;
    }

    public FileMeta(boolean isDir) {
        this.isDir = isDir;
    }

    public FileMeta(DataType dataType, Map<String, String> tag) {
        this.dataType = dataType;
        this.tag = tag;
    }

    public boolean ifTagEqual(Map<String, String> map) {
        if (map.size() != tag.size()) return false;

        for (String key : map.keySet()) {
            if (!tag.containsKey(key)) return false;
            String value1 = map.get(key);
            String value2 = tag.get(key);
            if (!value1.equals(value2)) return false;
        }

        return true;
    }

    public boolean ifContainTag(Map<String, String> map) {
        for (String key : map.keySet()) {
            if (this.tag==null || !this.tag.containsKey(key)) return false;
            String value = map.get(key);
            if (!this.tag.get(key).equals(value)) return false;
        }
        return true;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Map<String, String> getTag() {
        return tag;
    }

    public void setTag(Map<String, String> tag) {
        this.tag = tag;
    }

    public boolean isDir() {
        return isDir;
    }

    public void setDir(boolean dir) {
        isDir = dir;
    }
}
