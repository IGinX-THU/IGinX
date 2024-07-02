package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

public enum FilterType {
  Key,
  Value,
  Path,
  Expr,
  Bool, // holder

  And,
  Or,
  Not;

  public static boolean isLeafFilter(FilterType filterType) {
    return filterType == Key || filterType == Value || filterType == Path || filterType == Expr;
  }

  public static boolean isCompoundFilter(FilterType filterType) {
    return filterType != Key && filterType != Value && filterType != Path;
  }

  public static boolean isTimeFilter(Filter filter) {
    switch (filter.getType()) {
      case Value:
        return false;
      case Key:
        return true;
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        return isTimeFilter(notFilter.getChild());
      case And:
        AndFilter andFilter = (AndFilter) filter;
        for (Filter f : andFilter.getChildren()) {
          if (!isTimeFilter(f)) {
            return false;
          }
        }
        break;
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        for (Filter f : orFilter.getChildren()) {
          if (!isTimeFilter(f)) {
            return false;
          }
        }
        break;
        // TODO: case label. should we return true?
      case Bool:
        break;
      case Path:
        break;
    }
    return true;
  }

  public static boolean isValueFilter(Filter filter) {
    switch (filter.getType()) {
      case Value:
        return true;
      case Key:
        return false;
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        return isValueFilter(notFilter.getChild());
      case And:
        AndFilter andFilter = (AndFilter) filter;
        for (Filter f : andFilter.getChildren()) {
          if (!isValueFilter(f)) {
            return false;
          }
        }
        break;
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        for (Filter f : orFilter.getChildren()) {
          if (!isValueFilter(f)) {
            return false;
          }
        }
        break;
      default: // TODO: case label
        break;
    }
    return true;
  }
}
