package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.*;
import cn.edu.tsinghua.iginx.engine.shared.function.tsbs.Transposition;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.TypeUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

import java.util.*;
import java.util.regex.Pattern;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;

public class PyUDSF implements UDSF {

    private static final Logger logger = LoggerFactory.getLogger(PyUDSF.class);

    private static final String PY_UDSF = "py_udsf";

    private final PythonInterpreter interpreter;

    private final String funcName;

    private MappingFunction function = null;

    public PyUDSF(PythonInterpreter interpreter, String funcName) {
        this.interpreter = interpreter;
        this.funcName = funcName;
        logger.info("[FaultToleranceQuery][Debug][UDF] load udf: {}", funcName);
        if (funcName.equals("transposition")) {
            this.function = Transposition.getInstance();
        }
    }

    @Override
    public RowStream transform(RowStream rows, Map<String, Value> params) throws Exception {
        if (function != null) {
            return function.transform(rows, params);
        }
        if (!isLegal(params)) {
                throw new IllegalArgumentException("unexpected params for PyUDSF.");
        }

        List<Object> names = new ArrayList<>();
        List<Object> types = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        if (rows.getHeader().hasKey()) {
            names.add(GlobalConstant.KEY_NAME);
            types.add(TypeUtils.getTypeValueFromDataType(DataType.LONG));
        }

        String target = params.get(PARAM_PATHS).getBinaryVAsString();
        if (StringUtils.isPattern(target)) {
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            boolean found = false;
            for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
                Field field = rows.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    names.add(field.getName());
                    types.add(TypeUtils.getTypeValueFromDataType(field.getType()));
                    indices.add(i);
                    found = true;
                }
            }
            if (!found) {
                return Table.EMPTY_TABLE;
            }
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Table.EMPTY_TABLE;
            }

            Field field = rows.getHeader().getField(index);
            names.add(field.getName());
            types.add(TypeUtils.getTypeValueFromDataType(field.getType()));
            indices.add(index);
        }


        List<List<Object>> data = new ArrayList<>();
        data.add(names);
        data.add(types);
        while (rows.hasNext()) {
            Row row = rows.next();
            List<Object> rowData = new ArrayList<>();
            if (rows.getHeader().hasKey()) {
                rowData.add(row.getKey());
            }
            for (Integer idx: indices) {
                rowData.add(row.getValues()[idx]);
            }
            data.add(rowData);
        }
        List<List<Object>> res = (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data);
        if (res == null || res.size() == 0) {
            return Table.EMPTY_TABLE;
        }

        List<Object> firstRow = res.get(0);
        List<Object> secondRow = res.get(1);
        Field key = null;
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < firstRow.size(); i++) {
            String name = (String) firstRow.get(i);
            DataType type = TypeUtils.getDataTypeFromTypeValue(secondRow.get(i));
            if (i == 0 && name.equals(GlobalConstant.KEY_NAME)) {
                key = Field.KEY;
                continue;
            }
            Field field = new Field(name, type);
            fields.add(field);
        }

        Header header = new Header(key, fields);
        List<Row> rowList = new ArrayList<>();
        for (int i = 2; i < res.size(); i++) {
            List<Object> valueList = res.get(i);
            if (key != null) {
                Object[] values = new Object[valueList.size() - 1];
                for (int j = 1; j < valueList.size(); j++) {
                    values[j - 1] = valueList.get(j);
                }
                rowList.add(new Row(header, (long) valueList.get(0), values));
            } else {
                Object[] values = new Object[valueList.size()];
                for (int j = 0; j < valueList.size(); j++) {
                    values[j] = valueList.get(j);
                }
                rowList.add(new Row(header, values));
            }
        }
        return new Table(header, rowList);
    }

    private boolean isLegal(Map<String, Value> params) {
        List<String> neededParams = Collections.singletonList(PARAM_PATHS);
        for (String param : neededParams) {
            if (!params.containsKey(param)) {
                return false;
            }
        }

        Value paths = params.get(PARAM_PATHS);
        return paths != null && paths.getDataType() == DataType.BINARY;
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
    public String getFunctionName() {
        return funcName;
    }
}
