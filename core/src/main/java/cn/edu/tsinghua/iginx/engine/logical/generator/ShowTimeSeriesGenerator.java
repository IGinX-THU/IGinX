package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowTimeSeriesGenerator extends AbstractGenerator {
@SuppressWarnings("unused")
private static final Logger logger = LoggerFactory.getLogger(ShowTimeSeriesGenerator.class);

private static final ShowTimeSeriesGenerator instance = new ShowTimeSeriesGenerator();

private ShowTimeSeriesGenerator() {
    this.type = GeneratorType.ShowTimeSeries;
}

public static ShowTimeSeriesGenerator getInstance() {
    return instance;
}

@Override
protected Operator generateRoot(Statement statement) {
    ShowColumnsStatement showColumnsStatement = (ShowColumnsStatement) statement;
    return new ShowTimeSeries(
        new GlobalSource(),
        showColumnsStatement.getPathRegexSet(),
        showColumnsStatement.getTagFilter(),
        showColumnsStatement.getLimit(),
        showColumnsStatement.getOffset());
}
}
