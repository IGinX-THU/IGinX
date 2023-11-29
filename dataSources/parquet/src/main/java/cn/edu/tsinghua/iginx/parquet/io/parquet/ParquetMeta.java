package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.io.FileMeta;
import cn.edu.tsinghua.iginx.parquet.io.parquet.impl.MetaUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.Type;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class ParquetMeta implements FileMeta {

    public static final String PATH_DELIMITER = ".";

    ParquetMetadata metaData;

    ParquetMeta(Path path, ParquetReadOptions options) throws IOException {
        InputFile input = new LocalInputFile(path);
        try (SeekableInputStream stream = input.newStream()) {
            this.metaData = ParquetFileReader.readFooter(input, options, stream);
        }
    }


    @Nullable
    @Override
    public Set<String> fields() {
        Set<String> result = new HashSet<>();
        for (String[] path : metaData.getFileMetaData().getSchema().getPaths()) {
            result.add(String.join(PATH_DELIMITER, path));
        }
        return result;
    }

    @Nullable
    @Override
    public DataType getType(@Nonnull String field) {
        String[] path = StringUtils.split(field, PATH_DELIMITER);
        Type type= metaData.getFileMetaData().getSchema().getType(path);
        if(!type.isPrimitive()){
            return null;
        }
        return MetaUtils.toIginxType(type.asPrimitiveType());
    }

    @Nullable
    @Override
    public Map<String, String> extra() {
        return metaData.getFileMetaData().getKeyValueMetaData();
    }
}
