package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowColumns;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowColumnsGenerator extends AbstractGenerator {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowColumnsGenerator.class);

  private static final ShowColumnsGenerator instance = new ShowColumnsGenerator();

  private ShowColumnsGenerator() {
    this.type = GeneratorType.ShowColumns;
  }

  public static ShowColumnsGenerator getInstance() {
    return instance;
  }

  @Override
  protected Operator generateRoot(Statement statement) {
    ShowColumnsStatement showColumnsStatement = (ShowColumnsStatement) statement;
    return new ShowColumns(
        new GlobalSource(),
        showColumnsStatement.getPathRegexSet(),
        showColumnsStatement.getTagFilter(),
        showColumnsStatement.getLimit(),
        showColumnsStatement.getOffset());
  }
}
