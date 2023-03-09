package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public abstract class BaseHistoryDataGenerator {
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
