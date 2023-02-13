package cn.edu.tsinghua.iginx.sharedstore.utils;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.sharedstore.SharedStore;
import cn.edu.tsinghua.iginx.sharedstore.SharedStoreManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RowStreamStoreUtils {

    private static final Logger logger = LoggerFactory.getLogger(RowStreamStoreUtils.class);

    private static final SharedStore sharedStore = SharedStoreManager.getInstance().getSharedStore(ConfigDescriptor.getInstance().getConfig().getSharedStorage());

    public static long estimateRowStreamSize(RowStreamHolder holder) throws PhysicalException {
        Table table = holder.getAsTable();
        long estimatedSize = 46L;
        try {
            Header header = table.getHeader();
            estimatedSize += JSON.toJSONBytes(header).length;
            List<Row> rows = table.getRows();
            if (!rows.isEmpty()) {
                int sampleCnt = Math.min(5, rows.size());
                double sampleSize = 0.0;
                for (int i = 0; i < sampleCnt; i++) {
                    sampleSize += JSON.toJSONBytes(rows.get(i)).length;
                }
                estimatedSize += (long)((sampleSize / sampleCnt + 1) * rows.size());
            }
        } finally {
            table.reset();
            holder.setTable(table);
        }
        return estimatedSize;
    }

    public static double estimatePersistAndSerializeTime(long size) {
        double sizeInMB = size * 1.0 / 1024 / 1024;
        double persistTime = sizeInMB * 0.8 + 1;
        double serializeTime = persistTime * 15;
        return persistTime + serializeTime;
    }

    public static double estimateLoadAndDeserializeTime(long size) {
        double sizeInMB = size * 1.0 / 1024 / 1024;
        double loadTime = sizeInMB * 1.64 + 2.6;
        double deserializeTime = loadTime * 15;
        return loadTime + deserializeTime;
    }

    public static long getRowStreamLines(RowStreamHolder holder) throws PhysicalException {
        Table table = holder.getAsTable();
        long size = table.getRowSize();
        table.reset();;
        holder.setTable(table);
        return size;
    }

    public static boolean storeRowStream(String key, RowStreamHolder holder) throws PhysicalException {
        Table table = holder.getAsTable();
        try {
            long startTime = System.currentTimeMillis();
            byte[] bytes = JSON.toJSONBytes(table);
            long serializeSpan = System.currentTimeMillis() - startTime;
            boolean success = sharedStore.put(key.getBytes(StandardCharsets.UTF_8), bytes);
            long storeSpan = System.currentTimeMillis() - (startTime + serializeSpan);
            logger.info("[LongQuery][RowStreamStoreUtils][storeRowStream][key={}] value size: {}, serialize span: {}ms, storeSpan = {}ms", key, bytes.length, serializeSpan, storeSpan);
            return success;
        } catch (Exception e) {
            logger.error("[LongQuery][RowStreamStoreUtils][storeRowStream][key={}] value persistence failure: {}", key, e);
            throw new PhysicalException(e);
        } finally {
            table.reset();
            holder.setTable(table);
        }
    }

    public static RowStream loadRowStream(String key) {
        try {
            long startTime = System.currentTimeMillis();
            byte[] bytes = sharedStore.get(key.getBytes(StandardCharsets.UTF_8));
            long loadSpan = System.currentTimeMillis() - startTime;
            if (bytes == null) {
                logger.info("[LongQuery][RowStreamStoreUtils][loadRowStream][key={}] value is not exists", key);
                return null;
            }
            Table table = JSON.parseObject(bytes, Table.class);
            List<Row> rows = table.getRows();
            for (Row row: rows) {
                row.setHeader(table.getHeader());
                Object[] values = row.getValues();
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        continue;
                    }
                    if (row.getField(i).getType() == DataType.LONG) {
                        values[i] = ((Number) values[i]).longValue();
                    } else if (row.getField(i).getType() == DataType.DOUBLE) {
                        values[i] = ((Number) values[i]).doubleValue();
                    } else if (row.getField(i).getType() == DataType.BINARY) {
                        JSONArray jsonArr = (JSONArray) values[i];
                        byte[] arr = new byte[jsonArr.size()];
                        for (int j = 0; j < jsonArr.size(); j++) {
                            int value = jsonArr.getInteger(j);
                            arr[j] = (byte) value;
                        }
                        values[i] = arr;
                    }
                }
            }
            long deserializeSpan = System.currentTimeMillis() - (startTime + loadSpan);
            logger.info("[LongQuery][RowStreamStoreUtils][loadRowStream][key={}] value size: {}, deserialize span: {}ms, loadSpan = {}ms", key, bytes.length, deserializeSpan, loadSpan);
            return table;
        } catch (Exception e) {
            logger.error("[LongQuery][RowStreamStoreUtils][loadRowStream][key={}] value load failure: {}", key, e);
        }
        return null;
    }

    public static boolean checkRowStream(String key) {
        return sharedStore.exists(key.getBytes(StandardCharsets.UTF_8));
    }

    public static String encodeKey(long queryId, int sequence) {
        return String.format("p_%d_%d", queryId, sequence);
    }


}
