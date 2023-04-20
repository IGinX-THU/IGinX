package cn.edu.tsinghua.iginx.filesystem.query;

import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.io.File;
import java.util.List;
import java.util.Map;

public class FSResultTable {
    private File file;
    private List<Record> val;
    private DataType dataType;
    private Map<String, String> tags;

    public FSResultTable(File file, List<Record> val) {
        this(file,val, val.isEmpty()?null:val.get(0).getDataType(), null);
    }

    public FSResultTable(File file,List<Record> val, Map<String, String> tags) {
        this(file,val, val.isEmpty()?null:val.get(0).getDataType(), tags);
    }

    public FSResultTable(File file,List<Record> val, DataType dataType, Map<String, String> tags) {
        this.file = file;
        this.val = val;
        this.dataType = dataType;
        this.tags = tags;
    }

    public List<Record> getVal() {
        return val;
    }

    public void setVal(List<Record> val) {
        this.val = val;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }
}
