package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public abstract class BaseHistoryDataGenerator {

    protected int portOri;

    public static final List<String> PATH_LIST_ORI =
            Arrays.asList("mn.wf01.wt01.status", "mn.wf01.wt01.temperature");

    public static final List<DataType> DATA_TYPE_LIST_ORI =
            Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

    public static final List<List<Object>> VALUES_LIST_ORI =
            Arrays.asList(Arrays.asList(true, 15.27), Arrays.asList(false, 20.71));

    protected int portExp;

    public static final List<String> PATH_LIST_EXP =
            Arrays.asList("mn.wf03.wt01.status", "mn.wf03.wt01.temperature");

    public static final List<DataType> DATA_TYPE_LIST_EXP =
            Arrays.asList(DataType.BOOLEAN, DataType.DOUBLE);

    public static final List<List<Object>> VALUES_LIST_EXP =
            Arrays.asList(Arrays.asList(true, 66.23), Arrays.asList(false, 77.71));

    public BaseHistoryDataGenerator() {}

    @Test
    public void oriHasDataExpHasData() {
        writeHistoryDataToOri();
        writeHistoryDataToExp();
    }

    @Test
    public void oriHasDataExpNoData() {
        writeHistoryDataToOri();
    }

    @Test
    public void oriNoDataExpHasData() {
        writeHistoryDataToExp();
    }

    @Test
    public void oriNoDataExpNoData() {}

    public abstract void writeHistoryDataToOri();

    public abstract void writeHistoryDataToExp();

    @Test
    public abstract void clearHistoryData();
}
