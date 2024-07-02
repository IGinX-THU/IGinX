package cn.edu.tsinghua.iginx.rest.bean;

import java.util.*;
import lombok.Data;

@Data
public class AnnotationLimit {
  private List<String> tag = new ArrayList<>(); // LHZ应当改为set集合
  private String text = ".*";
  private String title = ".*";

  public void addTag(String key) {
    tag.add(key);
  }
}
