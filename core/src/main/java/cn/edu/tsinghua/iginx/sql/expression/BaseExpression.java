package cn.edu.tsinghua.iginx.sql.expression;

public class BaseExpression implements Expression {

private final String pathName;
private String alias;

public BaseExpression(String pathName) {
    this(pathName, "");
}

public BaseExpression(String pathName, String alias) {
    this.pathName = pathName;
    this.alias = alias;
}

public String getPathName() {
    return pathName;
}

public String getAlias() {
    return alias;
}

public void setAlias(String alias) {
    this.alias = alias;
}

@Override
public String getColumnName() {
    return pathName;
}

@Override
public ExpressionType getType() {
    return ExpressionType.Base;
}

public boolean hasAlias() {
    return alias != null && !alias.equals("");
}
}
