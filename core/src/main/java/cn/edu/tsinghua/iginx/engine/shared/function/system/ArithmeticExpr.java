package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import java.util.Collections;

public class ArithmeticExpr implements RowMappingFunction {

  public static final String ARITHMETIC_EXPR = "arithmetic_expr";

  private static final ArithmeticExpr INSTANCE = new ArithmeticExpr();

  private ArithmeticExpr() {}

  public static ArithmeticExpr getInstance() {
    return INSTANCE;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.RowMapping;
  }

  @Override
  public String getIdentifier() {
    return ARITHMETIC_EXPR;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    if (params.getExpr() == null) {
      throw new IllegalArgumentException("unexpected params for arithmetic_expr.");
    }
    Expression expr = params.getExpr();

    Value ret = ExprUtils.calculateExpr(row, expr);
    if (ret == null) {
      return Row.EMPTY_ROW;
    }

    Field targetField = new Field(expr.getColumnName(), ret.getDataType());

    Header header =
        row.getHeader().hasKey()
            ? new Header(Field.KEY, Collections.singletonList(targetField))
            : new Header(Collections.singletonList(targetField));

    return new Row(header, row.getKey(), new Object[] {ret.getValue()});
  }
}
