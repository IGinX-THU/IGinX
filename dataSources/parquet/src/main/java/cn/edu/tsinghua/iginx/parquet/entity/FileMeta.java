package cn.edu.tsinghua.iginx.parquet.entity;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.CMD_DELETE;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileMeta {

    private String extraPath;

    private String dataPath;

    private final long startKey;

    private final long endKey;

    private final Map<String, DataType> pathMap;

    private final Map<String, List<KeyRange>> deleteRanges;

    public FileMeta(long startKey, long endKey, Map<String, DataType> pathMap) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.pathMap = pathMap;
        this.deleteRanges = new HashMap<>();
    }

    public FileMeta(
            String extraPath,
            String dataPath,
            long startKey,
            long endKey,
            Map<String, DataType> pathMap,
            Map<String, List<KeyRange>> deleteRanges) {
        this.extraPath = extraPath;
        this.dataPath = dataPath;
        this.startKey = startKey;
        this.endKey = endKey;
        this.pathMap = pathMap;
        this.deleteRanges = deleteRanges;
    }

    public void deleteData(List<String> paths, List<KeyRange> keyRanges) throws IOException {
        if (keyRanges == null || keyRanges.size() == 0) {
            for (String path : paths) {
                pathMap.remove(path);
                deleteRanges.remove(path);
            }
        } else {
            for (String path : paths) {
                if (deleteRanges.containsKey(path)) {
                    deleteRanges.get(path).addAll(keyRanges);
                } else {
                    deleteRanges.put(path, new ArrayList<>(keyRanges));
                }
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append(CMD_DELETE).append(" ");
        for (String path : paths) {
            builder.append(path).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append("#");
        if (keyRanges != null) {
            for (KeyRange keyRange : keyRanges) {
                builder.append(keyRange.getBeginKey())
                        .append(",")
                        .append(keyRange.getEndKey())
                        .append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("\n");

        File file = new File(extraPath);
        FileOutputStream fos = new FileOutputStream(file, true);
        OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
        osw.write(builder.toString());

        osw.flush();
        osw.close();
        fos.close();
    }

    public void setExtraPath(String extraPath) {
        this.extraPath = extraPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getExtraPath() {
        return extraPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public long getStartKey() {
        return startKey;
    }

    public long getEndKey() {
        return endKey;
    }

    public Map<String, DataType> getPathMap() {
        return pathMap;
    }

    public Map<String, List<KeyRange>> getDeleteRanges() {
        return deleteRanges;
    }
}
