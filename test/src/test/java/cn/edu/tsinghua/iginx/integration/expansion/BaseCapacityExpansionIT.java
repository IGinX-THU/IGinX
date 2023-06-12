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

    void addStorageEngineWithPrefix(String dataPrefix, String schemaPrefix);

    void addStorageEngine(boolean hasData);

    int getPort();
}
