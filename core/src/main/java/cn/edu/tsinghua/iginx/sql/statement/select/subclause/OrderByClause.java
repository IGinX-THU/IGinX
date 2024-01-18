package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import java.util.ArrayList;
import java.util.List;

public class OrderByClause {
  private final List<String> orderByPaths;
  private boolean ascending;

  public OrderByClause(List<String> orderByPaths, boolean ascending) {
    this.orderByPaths = orderByPaths;
    this.ascending = ascending;
  }

  public OrderByClause() {
    this.orderByPaths = new ArrayList<>();
    this.ascending = true;
  }

  public List<String> getOrderByPaths() {
    return orderByPaths;
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public void setOrderByPaths(String orderByPath) {
    this.orderByPaths.add(orderByPath);
  }
}
