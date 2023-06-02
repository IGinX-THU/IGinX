package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public interface BaseCapacityExpansionIT {

    @Test
    void testOriHasDataExpHasData();

    @Test
    void testOriHasDataExpNoData();

    @Test
    void testOriNoDataExpHasData();

    @Test
    void testOriNoDataExpNoData();

    @Test
    void testPrefixAndRemoveHistoryDataSource();

    void addStorageWithPrefix(String dataPrefix, String schemaPrefix);

    int getPort();
}
