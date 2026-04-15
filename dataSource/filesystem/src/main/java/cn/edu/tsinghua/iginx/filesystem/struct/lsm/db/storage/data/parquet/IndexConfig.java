package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.google.common.collect.Range;
import com.typesafe.config.Config;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class IndexConfig extends AbstractConfig {

    @Optional
    double selectivityThreshold = 0.1;

    @Optional
    long hitCountThreshold = 3;

    @Optional
    long bitmapCardinalityThreshold = 1000;

    @Override
    public List<ValidationProblem> validate() {
        List<ValidationProblem> problems = new ArrayList<>();
        validateInRange(problems, Fields.selectivityThreshold, Range.closed(0.0,1.0), selectivityThreshold);
        return problems;
    }

    public static ParquetConfig of(Config config) {
        return of(config, ParquetConfig.class);
    }
}
