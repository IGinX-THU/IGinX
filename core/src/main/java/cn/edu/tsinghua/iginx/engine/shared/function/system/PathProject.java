package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.PARAM_PATHS;

public class PathProject implements RowMappingFunction {

    public static final String PATH_PROJECT = "path_project";

    private static final PathProject INSTANCE = new PathProject();

    private PathProject() {}

    public static PathProject getInstance() {
        return INSTANCE;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.System;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.RowMapping;
    }

    @Override
    public String getIdentifier() {
        return PATH_PROJECT;
    }

    @Override
    public Row transform(Row row, Map<String, Value> params) throws Exception {
        if (params.size() == 0 || params.size() > 2) {
            throw new IllegalArgumentException("unexpected params for path project.");
        }
        String path = params.get(PARAM_PATHS).getBinaryVAsString();

        List<Field> targetFields = new ArrayList<>();
        List<Object> targetValues = new ArrayList<>();
        if (path.endsWith(ALL_PATH_SUFFIX)) {
            String prefix = path.substring(0, path.length() - 2);
            for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
                if (row.getField(i).getName().startsWith(prefix)) {
                    targetFields.add(row.getField(i));
                    targetValues.add(row.getValue(i));
                }
            }
            if (targetValues.size() == 0) {
                return Row.EMPTY_ROW;
            }
        } else {
            int index = row.getHeader().indexOf(path);
            if (index == -1) {
                return Row.EMPTY_ROW;
            }
            targetFields.add(row.getField(index));
            targetValues.add(row.getValue(index));
        }
        Header targetHeader =
                row.getHeader().hasKey()
                        ? new Header(Field.KEY, targetFields)
                        : new Header(targetFields);
        return new Row(targetHeader, row.getKey(), targetValues.toArray());
    }
}
