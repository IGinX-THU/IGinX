package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RenameLazyStream extends UnaryLazyStream {

  private final Rename rename;

  private Header header;

  public RenameLazyStream(Rename rename, RowStream stream) {
    super(stream);
    this.rename = rename;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      Header header = stream.getHeader();
      Map<String, String> aliasMap = rename.getAliasMap();

      List<Field> fields = new ArrayList<>();
      header
          .getFields()
          .forEach(
              field -> {
                // 如果列名在ignorePatterns中，对该列不执行rename
                for (String ignorePattern : rename.getIgnorePatterns()) {
                  if (StringUtils.match(field.getName(), ignorePattern)) {
                    fields.add(field);
                    return;
                  }
                }
                String alias = "";
                for (String oldPattern : aliasMap.keySet()) {
                  String newPattern = aliasMap.get(oldPattern);
                  if (oldPattern.equals("*") && newPattern.endsWith(".*")) {
                    String newPrefix = newPattern.substring(0, newPattern.length() - 1);
                    alias = newPrefix + field.getName();
                  } else if (oldPattern.endsWith(".*") && newPattern.endsWith(".*")) {
                    String oldPrefix = oldPattern.substring(0, oldPattern.length() - 1);
                    String newPrefix = newPattern.substring(0, newPattern.length() - 1);
                    if (field.getName().startsWith(oldPrefix)) {
                      alias = field.getName().replaceFirst(oldPrefix, newPrefix);
                    }
                    break;
                  } else if (oldPattern.equals(field.getFullName())) {
                    alias = newPattern;
                    break;
                  } else {
                    if (StringUtils.match(field.getName(), oldPattern)) {
                      if (newPattern.endsWith("." + oldPattern)) {
                        String prefix =
                            newPattern.substring(0, newPattern.length() - oldPattern.length());
                        alias = prefix + field.getName();
                      } else {
                        alias = newPattern;
                      }
                      break;
                    }
                  }
                }
                if (alias.isEmpty()) {
                  fields.add(field);
                } else {
                  fields.add(new Field(alias, field.getType(), field.getTags()));
                }
              });

      this.header = new Header(header.getKey(), fields);
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return stream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    Row row = stream.next();
    if (header.hasKey()) {
      return new Row(header, row.getKey(), row.getValues());
    } else {
      return new Row(header, row.getValues());
    }
  }
}
