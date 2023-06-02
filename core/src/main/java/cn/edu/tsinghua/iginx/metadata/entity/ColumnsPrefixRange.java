package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.alibaba.fastjson2.annotation.JSONType;

@JSONType(typeName = "ColumnsPrefixRange")
public class ColumnsPrefixRange implements ColumnsRange {

    private String column;

    private final TYPE type = TYPE.PREFIX;

    private boolean isClosed;

    private String schemaPrefix = null;

    public ColumnsPrefixRange(String column) {
        this.column = column;
    }

    public ColumnsPrefixRange(String column, String schemaPrefix) {
        this.column = column;
        this.schemaPrefix = schemaPrefix;
    }

    public ColumnsPrefixRange(String column, boolean isClosed) {
        this.column = column;
        this.isClosed = isClosed;
    }

    private String realColumn(String column) {
        if (column != null && schemaPrefix != null) return schemaPrefix + "." + column;
        return column;
    }

    @Override
    public boolean isContain(String colName) {
        // judge if is the dummy node && it will have specific prefix
        String timeSeries = realColumn(this.column);

        return (timeSeries == null
                || (colName != null && StringUtils.compare(colName, timeSeries) == 0));
    }

    @Override
    public boolean isIntersect(ColumnsRange colRange) {
        // judge if is the dummy node && it will have specific prefix
        String timeSeries = realColumn(this.column);

        return (colRange.getStartColumn() == null
                        || timeSeries == null
                        || StringUtils.compare(colRange.getStartColumn(), timeSeries) <= 0)
                && (colRange.getEndColumn() == null
                        || timeSeries == null
                        || StringUtils.compare(colRange.getEndColumn(), timeSeries) >= 0);
    }

    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setClosed(boolean closed) {
        this.isClosed = closed;
    }

    @Override
    public TYPE getType() {
        return type;
    }

    @Override
    public int compareTo(ColumnsRange o) {
        return 0;
    }

    @Override
    public String getSchemaPrefix() {
        return schemaPrefix;
    }

    @Override
    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }
}
