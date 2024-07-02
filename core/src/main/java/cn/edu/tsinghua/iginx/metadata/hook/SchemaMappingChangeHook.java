package cn.edu.tsinghua.iginx.metadata.hook;

import java.util.Map;

public interface SchemaMappingChangeHook {

  void onChange(String schema, Map<String, Integer> schemaMapping);
}
