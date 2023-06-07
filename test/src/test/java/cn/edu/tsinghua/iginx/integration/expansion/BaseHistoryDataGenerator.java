package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public abstract class BaseHistoryDataGenerator {

    protected int portOri;

    protected List<String> pathListOri = new ArrayList<>();

    protected List<DataType> dataTypeListOri = new ArrayList<>();

    protected List<List<Object>> valuesListOri = new ArrayList<>();

    protected int portExp;

    protected List<String> pathListExp = new ArrayList<>();

    protected List<DataType> dataTypeListExp = new ArrayList<>();

    protected List<List<Object>> valuesListExp = new ArrayList<>();

    public BaseHistoryDataGenerator() {
        pathListOri.add("mn.wf01.wt01.status");
        pathListOri.add("mn.wf01.wt01.temperature");

        dataTypeListOri.add(DataType.BOOLEAN);
        dataTypeListOri.add(DataType.DOUBLE);

        valuesListOri.add(
                new ArrayList<Object>() {
                    {
                        add(true);
                        add(null);
                    }
                });
        valuesListOri.add(
                new ArrayList<Object>() {
                    {
                        add(false);
                        add(20.71);
                    }
                });

        pathListExp.add("mn.wf03.wt01.status");
        pathListExp.add("mn.wf03.wt01.temperature");

        dataTypeListExp.add(DataType.BOOLEAN);
        dataTypeListExp.add(DataType.DOUBLE);

        valuesListExp.add(
                new ArrayList<Object>() {
                    {
                        add(true);
                        add(null);
                    }
                });
        valuesListExp.add(
                new ArrayList<Object>() {
                    {
                        add(false);
                        add(77.71);
                    }
                });
    }

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

    public void writeHistoryDataToOri() {}

    public void writeHistoryDataToExp() {}

    @Test
    public void clearHistoryData() {}
}
