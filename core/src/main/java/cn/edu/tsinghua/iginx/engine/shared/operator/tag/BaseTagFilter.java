package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

public class BaseTagFilter implements TagFilter {

  private final String tagKey;

  private final String tagValue;

  public BaseTagFilter(String tagKey, String tagValue) {
    this.tagKey = tagKey;
    this.tagValue = tagValue;
  }

  public String getTagKey() {
    return tagKey;
  }

  public String getTagValue() {
    return tagValue;
  }

  @Override
  public TagFilterType getType() {
    return TagFilterType.Base;
  }

  @Override
  public TagFilter copy() {
    return new BaseTagFilter(tagKey, tagValue);
  }

  @Override
  public String toString() {
    return tagKey + "=\"" + tagValue + "\"";
  }
}
