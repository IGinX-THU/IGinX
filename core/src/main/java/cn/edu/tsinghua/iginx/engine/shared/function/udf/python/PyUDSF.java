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
        if (StringUtils.isPattern(target)) {
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            List<String> name = new ArrayList<>();
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
                Field field = rows.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    name.add(getFunctionName() + "(" + field.getName() + ")");
                    indices.add(i);
                }
            }
            if (name.isEmpty()) {
                return Table.EMPTY_TABLE;
            }

            List<List<Object>> data = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                List<Object> rowData = new ArrayList<>();
                for (Integer idx: indices) {
                    rowData.add(row.getValues()[idx]);
                }
                data.add(rowData);
            }
            List<List<Object>> res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
            if (res == null || res.size() == 0) {
                return Table.EMPTY_TABLE;
            }
            interpreters.add(interpreter);

            List<Object> firstRow = res.get(0);
            List<Field> targetFields = new ArrayList<>();
            for (int i = 0; i < name.size(); i++) {
                targetFields.add(new Field(name.get(i), TypeUtils.getDataTypeFromObject(firstRow.get(i))));
            }
            Header header = new Header(targetFields);
            return RowUtils.constructNewTable(header, res);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Table.EMPTY_TABLE;
            }

            List<List<Object>> data = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                data.add(Collections.singletonList(row.getValues()[index]));
            }
            List<List<Object>> res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
            if (res == null || res.size() == 0) {
                return Table.EMPTY_TABLE;
            }
            interpreters.add(interpreter);

            List<Object> firstRow = res.get(0);
            Field targetField = new Field(getFunctionName() + "(" + target + ")", TypeUtils.getDataTypeFromObject(firstRow.get(0)));
            Header header = new Header(Collections.singletonList(targetField));
            return RowUtils.constructNewTable(header, res);
        }
    }

    @Override
    public String getFunctionName() {
        return funcName;
    }
}
