package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.PARAM_PATHS;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CheckUtils {

    public static <T> List<T> castList(Object obj, Class<T> clazz) {
        List<T> result = new ArrayList<T>();
        if (obj instanceof List<?>) {
            for (Object o : (List<?>) obj) {
                result.add(clazz.cast(o));
            }
            return result;
        }
        return null;
    }

    public static boolean isLegal(Map<String, Value> params) {
        List<String> neededParams = Collections.singletonList(PARAM_PATHS);
        for (String param : neededParams) {
            if (!params.containsKey(param)) {
                return false;
            }
        }

        Value paths = params.get(PARAM_PATHS);
        return paths != null && paths.getDataType() == DataType.BINARY;
    }
}
