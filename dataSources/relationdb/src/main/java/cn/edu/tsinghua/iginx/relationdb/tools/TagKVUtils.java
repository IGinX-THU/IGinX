package cn.edu.tsinghua.iginx.relationdb.tools;

import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.*;

//import static cn.edu.tsinghua.iginx.relationDB.tools.Constants.RELATIONDB_SEPARATOR;

public class TagKVUtils {

    public static Pair<String, Map<String, String>> splitFullName(String fullName,String RELATIONDB_SEPARATOR) {
        if (!fullName.contains(RELATIONDB_SEPARATOR)) {
            return new Pair<>(fullName, null);
        }

        String[] parts = fullName.split("\\" + RELATIONDB_SEPARATOR);
        String name = parts[0];

        Map<String, String> tags = new HashMap<>();
        for (int i = 1; i < parts.length; i++) {
            if (i % 2 != 0) {
                continue;
            }
            String tagKey = parts[i - 1];
            String tagValue = parts[i];
            tags.put(tagKey, tagValue);
        }
        return new Pair<>(name, tags);
    }

    public static String toFullName(String name, Map<String, String> tags,String RELATIONDB_SEPARATOR) {
        if (tags != null && !tags.isEmpty()) {
            TreeMap<String, String> sortedTags = new TreeMap<>(tags);
            StringBuilder pathBuilder = new StringBuilder();
            sortedTags.forEach((tagKey, tagValue) -> {
                pathBuilder.append(RELATIONDB_SEPARATOR).append(tagKey).append(RELATIONDB_SEPARATOR).append(tagValue);
            });
            name += pathBuilder.toString();
        }
        return name;
    }
}
