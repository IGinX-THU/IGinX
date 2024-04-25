package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultUtils {

  private static final Logger logger = LoggerFactory.getLogger(ResultUtils.class);

  public static void setEmptyQueryResp(
      RequestContext ctx, List<String> paths, List<DataType> types) {
    Result result = new Result(RpcUtils.SUCCESS);
    result.setKeys(new Long[0]);
    result.setValuesList(new ArrayList<>());
    result.setBitmapList(new ArrayList<>());
    result.setPaths(paths);
    result.setDataTypes(types);
    ctx.setResult(result);
  }

  public static void setResultFromRowStream(RequestContext ctx, RowStream stream)
      throws PhysicalException {
    Result result = null;
    if (ctx.isUseStream()) {
      Status status = RpcUtils.SUCCESS;
      if (ctx.getWarningMsg() != null && !ctx.getWarningMsg().isEmpty()) {
        status = new Status(StatusCode.PARTIAL_SUCCESS.getStatusCode());
        status.setMessage(ctx.getWarningMsg());
      }
      result = new Result(status);
      result.setResultStream(stream);
      ctx.setResult(result);
      return;
    }

    if (stream == null) {
      setEmptyQueryResp(ctx, new ArrayList<>(), new ArrayList<>());
      return;
    }

    List<String> paths = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();
    List<DataType> types = new ArrayList<>();
    stream
        .getHeader()
        .getFields()
        .forEach(
            field -> {
              paths.add(field.getFullName());
              types.add(field.getType());
              if (field.getTags() == null) {
                tagsList.add(new HashMap<>());
              } else {
                tagsList.add(field.getTags());
              }
            });

    List<Long> timestampList = new ArrayList<>();
    List<ByteBuffer> valuesList = new ArrayList<>();
    List<ByteBuffer> bitmapList = new ArrayList<>();

    boolean hasTimestamp = stream.getHeader().hasKey();
    while (stream.hasNext()) {
      Row row = stream.next();

      Object[] rowValues = row.getValues();
      valuesList.add(ByteUtils.getRowByteBuffer(rowValues, types));

      Bitmap bitmap = new Bitmap(rowValues.length);
      for (int i = 0; i < rowValues.length; i++) {
        if (rowValues[i] != null) {
          bitmap.mark(i);
        }
      }
      bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));

      if (hasTimestamp) {
        timestampList.add(row.getKey());
      }
    }

    if (valuesList.isEmpty()) { // empty result
      setEmptyQueryResp(ctx, paths, types);
      return;
    }

    Status status = RpcUtils.SUCCESS;
    if (ctx.getWarningMsg() != null && !ctx.getWarningMsg().isEmpty()) {
      status = new Status(StatusCode.PARTIAL_SUCCESS.getStatusCode());
      status.setMessage(ctx.getWarningMsg());
    }
    result = new Result(status);
    if (timestampList.size() != 0) {
      Long[] timestamps = timestampList.toArray(new Long[timestampList.size()]);
      result.setKeys(timestamps);
    }
    result.setValuesList(valuesList);
    result.setBitmapList(bitmapList);
    result.setPaths(paths);
    result.setTagsList(tagsList);
    result.setDataTypes(types);
    ctx.setResult(result);

    stream.close();
  }

  public static void setShowTSRowStreamResult(RequestContext ctx, RowStream stream)
      throws PhysicalException {
    if (ctx.isUseStream()) {
      Result result = new Result(RpcUtils.SUCCESS);
      result.setResultStream(stream);
      ctx.setResult(result);
      return;
    }
    List<String> paths = new ArrayList<>();
    // todo:need physical layer to support.
    List<Map<String, String>> tagsList = new ArrayList<>();
    List<DataType> types = new ArrayList<>();

    while (stream.hasNext()) {
      Row row = stream.next();
      Object[] rowValues = row.getValues();

      if (rowValues.length == 2) {
        paths.add(new String((byte[]) rowValues[0]));
        DataType type = DataTypeUtils.getDataTypeFromString(new String((byte[]) rowValues[1]));
        if (type == null) {
          logger.warn("unknown data type [{}]", rowValues[1]);
        }
        types.add(type);
      } else {
        logger.warn("show columns result col size = {}", rowValues.length);
      }
    }

    Result result = new Result(RpcUtils.SUCCESS);
    result.setPaths(paths);
    result.setTagsList(tagsList);
    result.setDataTypes(types);
    ctx.setResult(result);
  }
}
