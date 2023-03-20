package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDTF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.TypeUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.concurrent.BlockingQueue;
import pemja.core.PythonInterpreter;

import java.util.*;
import java.util.regex.Pattern;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;

public class PyUDTF implements UDTF {

    private static final String PY_UDTF = "py_udtf";

    private final BlockingQueue<PythonInterpreter> interpreters;

    private final String funcName;

    public PyUDTF(BlockingQueue<PythonInterpreter> interpreters, String funcName) {
        this.interpreters = interpreters;
        this.funcName = funcName;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.UDF;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.RowMapping;
    }

    @Override
    public String getIdentifier() {
        return PY_UDTF;
    }

    @Override
    public Row transform(Row row, Map<String, Value> params) throws Exception {
        if (!CheckUtils.isLegal(params)) {
            throw new IllegalArgumentException("unexpected params for PyUDTF.");
        }

        PythonInterpreter interpreter = interpreters.take();

        String target = params.get(PARAM_PATHS).getBinaryVAsString();
        List<List<Object>> res;
        if (StringUtils.isPattern(target)) {
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            List<Object> colNames = new ArrayList<>();
            List<Object> colTypes = new ArrayList<>();
            List<Object> rowData = new ArrayList<>();
            for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
                Field field = row.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    colNames.add(field.getName());
                    colTypes.add(field.getType().toString());
                    rowData.add(row.getValues()[i]);
                }
            }
            if (colNames.isEmpty()) {
                return Row.EMPTY_ROW;
            }

            List<List<Object>> data = new ArrayList<>();
            data.add(colNames);
            data.add(colTypes);
            data.add(rowData);
            res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
        } else {
            int index = row.getHeader().indexOf(target);
            if (index == -1) {
                return Row.EMPTY_ROW;
            }
            List<List<Object>> data = Arrays.asList(
                Collections.singletonList(target),
                Collections.singletonList(row.getField(index).getType().toString()),
                Collections.singletonList(row.getValues()[index])
            );

            res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
        }

        if (res == null || res.size() < 3) {
            return Row.EMPTY_ROW;
        }
        interpreters.add(interpreter);

        Header header = RowUtils.constructHeaderWithFirstTwoRows(res, row.getHeader().hasKey());
        return RowUtils.constructNewRowWithKey(header, row.getKey(), res.get(2));
    }

    @Override
    public String getFunctionName() {
        return funcName;
    }
}
