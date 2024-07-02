package cn.edu.tsinghua.iginx.rest.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Data;

@Data
public class Metric {
  private String name;
  private Long startAbsolute;
  private Long endAbsolute;
  private Map<String, String> tags = new TreeMap<>();
  private List<Long> keys = new ArrayList<>();
  private List<String> values = new ArrayList<>();
  private Map<String, String> anno = new HashMap<>();
  private String annotation = null;

  public void addTag(String key, String value) {
    tags.put(key, value);
  }

  public void addKey(Long key) {
    keys.add(key);
  }

  public void addValue(String value) {
    values.add(value);
  }

  public void addAnno(String key, String value) {
    anno.put(key, value);
  }
}
