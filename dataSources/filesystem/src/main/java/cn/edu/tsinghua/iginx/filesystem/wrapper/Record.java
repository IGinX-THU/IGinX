package cn.edu.tsinghua.iginx.filesystem.wrapper;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Record {
    private static final Logger logger = LoggerFactory.getLogger(Record.class);
    private int MAXDATASETLEN = 100;
    public static int preperty = 100;
    private long key;
    private DataType dataType;
    private Object rawData;
    private int index = 0;

    public Record(long key, DataType dataType, Object rawData) {
        this.key = key;
        this.dataType = dataType;
        this.rawData = rawData;
    }

    public Record(long key, Object rawData) {
        this.key = key;
        this.rawData = rawData;
        this.dataType = DataType.BINARY;
    }

    //    public void addRawDataRecord(List<Byte> oriData, int beg, int end) {
//        for(int i = beg; i<end; i++) {
//            rawData[index++] = oriData.get(i);
//            if(index >= MAXDATASETLEN) {
//                logger.error("load the data size large than max size {}, had loaded at {} ", MAXDATASETLEN, i);
//                break;
//            }
//        }
//    }

    public String toString() {
//        StringBuilder res = new StringBuilder();
//        for (int i = 0; i < index; i++) {
//            res.append(rawData[i] + ",");
//        }
//        res.deleteCharAt(res.length() - 1);
//        return res.toString();
        return null;
    }

    public Object getRawData() {
        return rawData;
    }

    public void setMAXDATASETLEN(int length) {
        MAXDATASETLEN = length;
    }

    public long getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public void setRawData(Object rawData) {
        this.rawData = rawData;
    }
}
