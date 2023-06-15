package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import java.nio.file.Path;
import java.util.List;

public class WritePlan {

private Path filePath;

private List<String> pathList;

private KeyInterval keyInterval;

public WritePlan(Path filePath, List<String> pathList, KeyInterval keyInterval) {
    this.filePath = filePath;
    this.pathList = pathList;
    this.keyInterval = keyInterval;
}

public Path getFilePath() {
    return filePath;
}

public void setFilePath(Path filePath) {
    this.filePath = filePath;
}

public List<String> getPathList() {
    return pathList;
}

public void setPathList(List<String> pathList) {
    this.pathList = pathList;
}

public KeyInterval getKeyInterval() {
    return keyInterval;
}

public void setKeyInterval(KeyInterval keyInterval) {
    this.keyInterval = keyInterval;
}
}
