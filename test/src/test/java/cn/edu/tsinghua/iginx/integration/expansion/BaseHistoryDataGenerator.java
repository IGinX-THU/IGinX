package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public abstract class BaseHistoryDataGenerator {

    protected Map<String, Pair<DataType, List<Pair<Long, Object>>>> seriesA = new HashMap<>();
    protected Map<String, Pair<DataType, List<Pair<Long, Object>>>> seriesB = new HashMap<>();

    public BaseHistoryDataGenerator() {
        seriesA.put(
                "ln.wf01.wt01.status",
                new Pair<>(
                        DataType.BOOLEAN,
                        new ArrayList<Pair<Long, Object>>() {
                            {
                                add(new Pair<>(100L, true));
                                add(new Pair<>(200L, false));
                            }
                        }));
        seriesA.put(
                "ln.wf01.wt01.temperature",
                new Pair<>(
                        DataType.DOUBLE,
                        new ArrayList<Pair<Long, Object>>() {
                            {
                                add(new Pair<>(200L, 20.71));
                            }
                        }));

        seriesB.put(
                "ln.wf03.wt01.status",
                new Pair<>(
                        DataType.BOOLEAN,
                        new ArrayList<Pair<Long, Object>>() {
                            {
                                add(new Pair<>(77L, true));
                                add(new Pair<>(200L, false));
                            }
                        }));
        seriesB.put(
                "ln.wf03.wt01.temperature",
                new Pair<>(
                        DataType.DOUBLE,
                        new ArrayList<Pair<Long, Object>>() {
                            {
                                add(new Pair<>(200L, 77.71));
                            }
                        }));
    }

    @Test
    public void oriHasDataExpHasData() throws Exception {
        writeHistoryDataToA();
        writeHistoryDataToB();
    }

    @Test
    public void oriHasDataExpNoData() throws Exception {
        writeHistoryDataToA();
    }

    @Test
    public void oriNoDataExpHasData() throws Exception {
        writeHistoryDataToB();
    }

    @Test
    public void oriNoDataExpNoData() throws Exception {}

    @Test
    public void writeHistoryDataToB() throws Exception {}

    @Test
    public void writeHistoryDataToA() throws Exception {}

    @Test
    public void clearData() {}
}
