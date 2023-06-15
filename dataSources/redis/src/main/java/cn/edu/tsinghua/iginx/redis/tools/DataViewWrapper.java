package cn.edu.tsinghua.iginx.redis.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.HashMap;
import java.util.Map;

public class DataViewWrapper {

private final DataView dataView;

private final Map<Integer, String> pathCache;

public DataViewWrapper(DataView dataView) {
    this.dataView = dataView;
    this.pathCache = new HashMap<>();
}

public int getPathNum() {
    return dataView.getPathNum();
}

public int getTimeSize() {
    return dataView.getKeySize();
}

public String getPath(int index) {
    if (pathCache.containsKey(index)) {
    return pathCache.get(index);
    }
    String path = dataView.getPath(index);
    Map<String, String> tags = dataView.getTags(index);
    path = TagKVUtils.toFullName(path, tags);
    pathCache.put(index, path);
    return path;
}

public DataType getDataType(int index) {
    return dataView.getDataType(index);
}

public Long getTimestamp(int index) {
    return dataView.getKey(index);
}

public Object getValue(int index1, int index2) {
    return dataView.getValue(index1, index2);
}

public BitmapView getBitmapView(int index) {
    return dataView.getBitmapView(index);
}

public Pair<Map<String, String>, Map<String, Double>> getPathData(int index) {
    Map<String, String> values = new HashMap<>();
    Map<String, Double> scores = new HashMap<>();
    Pair<Map<String, String>, Map<String, Double>> ret = new Pair<>(values, scores);

    if (dataView.isRowData()) {
    for (int i = 0; i < dataView.getKeySize(); i++) {
        BitmapView bitmapView = dataView.getBitmapView(i);
        if (bitmapView.get(index)) {
        int nonNullCnt = 0;
        for (int j = 0; j < index; j++) {
            if (bitmapView.get(j)) {
            nonNullCnt++;
            }
        }
        String key = String.valueOf(dataView.getKey(i));
        double score = dataView.getKey(i);
        String value = DataTransformer.objectValueToString(dataView.getValue(i, nonNullCnt));
        values.put(key, value);
        scores.put(key, score);
        }
    }
    } else {
    int nonNullCnt = 0;
    BitmapView bitmapView = dataView.getBitmapView(index);
    for (int i = 0; i < dataView.getKeySize(); i++) {
        if (bitmapView.get(i)) {
        String key = String.valueOf(dataView.getKey(i));
        double score = dataView.getKey(i);
        String value = DataTransformer.objectValueToString(dataView.getValue(index, nonNullCnt));
        values.put(key, value);
        scores.put(key, score);
        nonNullCnt++;
        }
    }
    }
    // redis will move key if value is empty.
    // so we set sentinel here.
    values.put("-1", "0");
    scores.put("-1", 0.0);
    return ret;
}
}
