import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import org.junit.Test;

public class ResultFormatTest {

  @Test
  public void SessionExecuteSqlResultFormatTest() {
    SqlType sqlType = SqlType.Query;
    long[] keys =
        new long[] {
          1679389046000L, 1679389047000L, 1679389048000L, 1679389049000L, 1679389050000L,
        };
    List<String> paths = Arrays.asList("cpu.usage", "cpu.info");
    List<List<Object>> data =
        Arrays.asList(
            Arrays.asList(0.32, "LOW"),
            Arrays.asList(0.12, "LOW"),
            Arrays.asList(0.67, "HIGH"),
            Arrays.asList(0.83, "HIGH"),
            Arrays.asList(0.55, "HIGH"));
    List<DataType> types = Arrays.asList(DataType.DOUBLE, DataType.BINARY);
    SessionExecuteSqlResult result = new SessionExecuteSqlResult(sqlType, keys, paths, data, types);

    List<String> timePrecisionList = Arrays.asList("ns", "us", "ms", "s");
    for (String timePrecision : timePrecisionList) {
      List<List<String>> cacheResult =
          result.getResultInList(true, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision);

      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(FormatUtils.DEFAULT_TIME_FORMAT);
      simpleDateFormat.setTimeZone(TimeZone.getDefault()); // 设置为系统当前时区

      for (int i = 0; i < keys.length; i++) {
        long timestamp =
            TimeUtils.getTimeInNs(keys[i], TimeUtils.strToTimePrecision(timePrecision));
        assertEquals(simpleDateFormat.format(timestamp), cacheResult.get(i + 1).get(0));
      }
    }
  }
}
