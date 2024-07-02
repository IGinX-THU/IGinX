/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.rest.bean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class Annotation {
  private static final Logger LOGGER = LoggerFactory.getLogger(Annotation.class);
  private List<String> tags = new ArrayList<>();
  private String text;
  private String title;
  private Long timestamp;
  private ObjectMapper mapper = new ObjectMapper();

  public Annotation(String str, Long tim) {
    timestamp = tim;
    try {
      JsonNode node = mapper.readTree(str);
      if (node == null) {
        return;
      }
      JsonNode text = node.get("description");
      if (text != null) {
        this.text = text.asText();
      }
      JsonNode title = node.get("title");
      if (title != null) {
        this.title = title.asText();
      }
      JsonNode tags = node.get("category");
      if (tags != null && tags.isArray()) {
        for (JsonNode tagsNode : tags) {
          this.tags.add(tagsNode.asText());
        }
      }
    } catch (Exception e) {
      LOGGER.error("Wrong annotation form in database", e);
    }
  }

  public boolean isEqual(Annotation p) {
    if (p.text.compareTo(text) != 0) {
      return true;
    }
    if (p.title.compareTo(title) != 0) {
      return true;
    }
    if (p.tags.size() != tags.size()) {
      return true;
    }
    for (int i = 0; i < p.tags.size(); i++) {
      if (p.tags.get(i).compareTo(tags.get(i)) != 0) {
        return true;
      }
    }
    return false;
  }

  public boolean match(AnnotationLimit annotationLimit) {
    if (!Pattern.matches(annotationLimit.getText(), text)) {
      return false;
    }
    if (!Pattern.matches(annotationLimit.getTitle(), title)) {
      return false;
    }
    // LHZ之后再改，目前没什么用
    //        for (String tag : tags) {
    //            if (Pattern.matches(annotationLimit.getTag(), tag)) {
    //                return true;
    //            }
    //        }
    return false;
  }
}
