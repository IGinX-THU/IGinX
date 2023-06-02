package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public interface BaseCapacityExpansionIT {

    @Test
    void oriHasDataExpHasData();

    @Test
    void oriHasDataExpNoData();

    @Test
    void oriNoDataExpHasData();

    @Test
    void oriNoDataExpNoData();

    @Test
    void testPrefixAndRemoveHistoryDataSource();

    void addStorageWithPrefix(String dataPrefix, String schemaPrefix);

    int getPort();
}
