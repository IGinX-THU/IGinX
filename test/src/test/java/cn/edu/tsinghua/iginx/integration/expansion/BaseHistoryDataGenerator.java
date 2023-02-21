package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public interface BaseHistoryDataGenerator {
    @Test
    default void oriHasDataExpHasData() throws Exception {
        writeHistoryDataToA();
        writeHistoryDataToB();
    }

    @Test
    default void oriHasDataExpNoData() throws Exception {
        writeHistoryDataToA();
    }

    @Test
    default void oriNoDataExpHasData() throws Exception {
        writeHistoryDataToB();
    }

    @Test
    default void oriNoDataExpNoData() throws Exception {
    }

    @Test
    public void writeHistoryDataToB() throws Exception;

    @Test
    public void writeHistoryDataToA() throws Exception;

    @Test
    public void clearData();
}
