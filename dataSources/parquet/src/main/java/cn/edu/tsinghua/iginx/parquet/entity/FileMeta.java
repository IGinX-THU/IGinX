package cn.edu.tsinghua.iginx.parquet.entity;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.CMD_DELETE;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
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

    private final long startTime;

    private final long endTime;

    private final Map<String, DataType> pathMap;

    private final Map<String, List<TimeRange>> deleteRanges;

    public FileMeta(long startTime, long endTime, Map<String, DataType> pathMap) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.pathMap = pathMap;
        this.deleteRanges = new HashMap<>();
    }

    public FileMeta(String extraPath, String dataPath, long startTime, long endTime,
        Map<String, DataType> pathMap,
        Map<String, List<TimeRange>> deleteRanges) {
        this.extraPath = extraPath;
        this.dataPath = dataPath;
        this.startTime = startTime;
        this.endTime = endTime;
        this.pathMap = pathMap;
        this.deleteRanges = deleteRanges;
    }

    public void deleteData(List<String> paths, List<TimeRange> timeRanges) throws IOException {
        if (timeRanges == null || timeRanges.size() == 0) {
            for (String path : paths) {
                pathMap.remove(path);
                deleteRanges.remove(path);
            }
        } else {
            for (String path : paths) {
                if (deleteRanges.containsKey(path)) {
                    deleteRanges.get(path).addAll(timeRanges);
                } else {
                    deleteRanges.put(path, new ArrayList<>(timeRanges));
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
        if (timeRanges != null) {
            for (TimeRange timeRange : timeRanges) {
                builder.append(timeRange.getBeginTime()).append(",").append(timeRange.getEndTime()).append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("\n");

        File file = new File(extraPath);
        FileOutputStream fos = new FileOutputStream(file,true);
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

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Map<String, DataType> getPathMap() {
        return pathMap;
    }

    public Map<String, List<TimeRange>> getDeleteRanges() {
        return deleteRanges;
    }
}
