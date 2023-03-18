package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.TypeUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.concurrent.BlockingQueue;
import pemja.core.PythonInterpreter;

import java.util.*;
import java.util.regex.Pattern;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;

public class PyUDSF implements UDSF {

    private static final String PY_UDSF = "py_udsf";

    private final BlockingQueue<PythonInterpreter> interpreters;

    private final String funcName;

    public PyUDSF(BlockingQueue<PythonInterpreter> interpreters, String funcName) {
        this.interpreters = interpreters;
        this.funcName = funcName;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.UDF;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.Mapping;
    }

    @Override
    public String getIdentifier() {
        return PY_UDSF;
    }

    @Override
    public RowStream transform(RowStream rows, Map<String, Value> params) throws Exception {
        if (!CheckUtils.isLegal(params)) {
            throw new IllegalArgumentException("unexpected params for PyUDSF.");
        }

        PythonInterpreter interpreter = interpreters.take();

        String target = params.get(PARAM_PATHS).getBinaryVAsString();
        List<List<Object>> res;
        if (StringUtils.isPattern(target)) {
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            List<Object> colNames = new ArrayList<>();
            List<Object> colTypes = new ArrayList<>();
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
                Field field = rows.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    colNames.add(field.getName());
                    colTypes.add(field.getType().toString());
                    indices.add(i);
                }
            }
            if (colNames.isEmpty()) {
                return Table.EMPTY_TABLE;
            }

            List<List<Object>> data = new ArrayList<>();
            data.add(colNames);
            data.add(colTypes);
            while (rows.hasNext()) {
                Row row = rows.next();
                List<Object> rowData = new ArrayList<>();
                for (Integer idx: indices) {
                    rowData.add(row.getValues()[idx]);
                }
                data.add(rowData);
            }
            res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Table.EMPTY_TABLE;
            }

            List<List<Object>> data = new ArrayList<>();
            data.add(Collections.singletonList(target));
            data.add(Collections.singletonList(rows.getHeader().getField(index).getType().toString()));
            while (rows.hasNext()) {
                Row row = rows.next();
                data.add(Collections.singletonList(row.getValues()[index]));
            }
            res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
        }

        if (res == null || res.size() < 3) {
            return Table.EMPTY_TABLE;
        }
        interpreters.add(interpreter);

        Header header = RowUtils.constructHeaderWithFirstTwoRows(res, false);
        return RowUtils.constructNewTable(header, res, 2);
    }

    @Override
    public String getFunctionName() {
        return funcName;
    }
}
