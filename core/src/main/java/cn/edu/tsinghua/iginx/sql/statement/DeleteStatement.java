package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends DataStatement {

    private boolean deleteAll; // delete data & path

    private List<String> paths;
    private List<KeyRange> keyRanges;
    private TagFilter tagFilter;

    private boolean involveDummyData;

    public DeleteStatement() {
        this.statementType = StatementType.DELETE;
        this.paths = new ArrayList<>();
        this.keyRanges = new ArrayList<>();
        this.deleteAll = false;
        this.tagFilter = null;
        this.involveDummyData = false;
    }

    public DeleteStatement(List<String> paths, long startKey, long endKey) {
        this.statementType = StatementType.DELETE;
        this.paths = paths;
        this.keyRanges = new ArrayList<>();
        this.keyRanges.add(new KeyRange(startKey, endKey));
        this.deleteAll = false;
        this.tagFilter = null;
        this.involveDummyData = false;
    }

    public DeleteStatement(List<String> paths) {
        this(paths, null);
    }

    public DeleteStatement(List<String> paths, TagFilter tagFilter) {
        this.statementType = StatementType.DELETE;
        this.paths = paths;
        this.keyRanges = new ArrayList<>();
        this.deleteAll = true;
        this.tagFilter = tagFilter;
        this.involveDummyData = false;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        paths.add(path);
    }

    public List<KeyRange> getKeyRanges() {
        return keyRanges;
    }

    public void setKeyRanges(List<KeyRange> keyRanges) {
        this.keyRanges = keyRanges;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    public void setTagFilter(TagFilter tagFilter) {
        this.tagFilter = tagFilter;
    }

    public boolean isInvolveDummyData() {
        return involveDummyData;
    }

    public void setInvolveDummyData(boolean involveDummyData) {
        this.involveDummyData = involveDummyData;
    }

    public void setKeyRangesByFilter(Filter filter) {
        if (filter != null) {
            this.keyRanges = ExprUtils.getKeyRangesFromFilter(filter);
            if (keyRanges.isEmpty()) {
                throw new SQLParserException(
                        "This clause delete nothing, check your filter again.");
            }
        }
    }

    public boolean isDeleteAll() {
        return deleteAll;
    }

    public void setDeleteAll(boolean deleteAll) {
        this.deleteAll = deleteAll;
    }
}
