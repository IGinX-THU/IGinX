package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import java.util.List;
import java.util.Objects;

public class GroupByKey {

    private final List<Object> groupByValues;  // the values of group by cols.

    private final int hash;

    public GroupByKey(List<Object> groupByValues) {
        this.groupByValues = groupByValues;
        this.hash = groupByValues.hashCode();
    }

    public List<Object> getGroupByValues() {
        return groupByValues;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupByKey that = (GroupByKey) o;
        return hash == that.hash && Objects.equals(groupByValues, that.groupByValues);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
