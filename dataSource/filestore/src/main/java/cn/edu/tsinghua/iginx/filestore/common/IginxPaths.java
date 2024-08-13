package cn.edu.tsinghua.iginx.filestore.common;

import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

public class IginxPaths {

  public static final String DOT = ".";

  private IginxPaths() {
  }

  @Nullable
  public static String get(String... paths) {
    return get(Arrays.asList(paths));
  }

  @Nullable
  public static String get(Iterable<? extends CharSequence> paths) {
    Iterable<? extends CharSequence> nonNullPaths = Iterables.filter(paths, Objects::nonNull);
    if (!nonNullPaths.iterator().hasNext()) {
      return null;
    }
    return String.join(DOT, nonNullPaths);
  }

  public static String get(Path path, String dot) {
    List<String> nodes = new ArrayList<>();
    for (Path fsNode : path) {
      nodes.add(fsNode.toString().replace(DOT, dot));
    }
    return get(nodes);
  }

  public static Path toFilePath(String path, String dot, FileSystem fs) {
    Pattern splitter = Pattern.compile(Pattern.quote(DOT));
    String[] nodes = splitter.split(path);
    String[] fsNodes = new String[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      fsNodes[i] = nodes[i].replace(dot, DOT);
    }
    return fs.getPath(fsNodes[0], Arrays.copyOfRange(fsNodes, 1, fsNodes.length));
  }


}
