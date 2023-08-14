package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import java.util.ArrayList;
import java.util.List;

public class DeleteColumnsStatement extends DataStatement {

  private final List<String> paths;

  private TagFilter tagFilter;

  public DeleteColumnsStatement() {
    this.statementType = StatementType.DELETE_COLUMNS;
    this.paths = new ArrayList<>();
    this.tagFilter = null;
  }

  public DeleteColumnsStatement(List<String> paths) {
    this.statementType = StatementType.DELETE_COLUMNS;
    this.paths = paths;
    this.tagFilter = null;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void addPath(String path) {
    this.paths.add(path);
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public void setTagFilter(TagFilter tagFilter) {
    this.tagFilter = tagFilter;
  }
}
