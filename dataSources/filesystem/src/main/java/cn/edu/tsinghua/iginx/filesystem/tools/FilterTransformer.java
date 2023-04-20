package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import com.alibaba.fastjson2.JSON;

import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

public class FilterTransformer {
    public static byte[] toBinary(Filter filter) {
        if (filter == null) {
            return null;
        }
        byte[] res = JsonUtils.toJson(filter);

        Stack<Filter> S = new Stack<>();
        S.push(filter);
        Integer index = 0;
        AndFilter andFilter = null;
        OrFilter orFilter = null;
        List<Filter> childs = null;
        while(!S.empty()) {
            boolean isLeafFilter = true;
            Filter tmpFilter = S.pop();
            if (!FilterType.isLeafFilter(tmpFilter.getType())) {
                switch (filter.getType()) {
                    case And:
                        andFilter = (AndFilter) filter;
                        childs = andFilter.getChildren();
                        isLeafFilter = false;
                        break;
                    case Or:
                        orFilter = (OrFilter) filter;
                        childs = orFilter.getChildren();
                        isLeafFilter = false;
                        break;
                }
            }
            StringBuilder tmpRes = new StringBuilder(new String(res));
            index = tmpRes.indexOf("{", index);
            res = addType(res, FilterType.getFilterClassName(tmpFilter.getType()), index++);
            if (isLeafFilter) {
                continue;
            }
            Collections.reverse(childs);
            if (andFilter != null) {
                S.addAll(childs);
            } else if (orFilter != null) {
                S.addAll(childs);
            }
        }
        return res;
    }

    private static byte[] addType(byte[] data, String typeName, Integer index) {
        return JsonUtils.addType("{", typeName, data, index);
    }

    public static Filter toFilter(byte[] filter) {
        return JsonUtils.fromJson(filter, Filter.class);
    }

    public static String toString(Filter filter) {
        if (filter == null) {
            return "";
        }
        switch (filter.getType()) {
            case And:
                return toString((AndFilter) filter);
            case Or:
                return toString((OrFilter) filter);
            case Not:
                return toString((NotFilter) filter);
            case Value:
                return toString((ValueFilter) filter);
            case Key:
                return toString((KeyFilter) filter);
            default:
                return "";
        }
    }

    private static String toString(AndFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" and ", "(", ")"));
    }

    private static String toString(OrFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" or ", "(", ")"));
    }
}
