package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BasePreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemImpl;
import cn.edu.tsinghua.iginx.filesystem.query.FSResultTable;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.jexl3.*;

public class FuncUT {

    @Test
    public void testCreateFile() throws IOException, PhysicalException {
        String path = "a.b.lhz";
        List<Record> valList = new ArrayList<>();
        FileSystemImpl fileSystem = new FileSystemImpl();
        Map<String, String> tags = new HashMap<>();
        tags.put("tt","v1");
        tags.put("tv","v2");
        valList.add(new Record(0, "no !".getBytes()));
        valList.add(new Record(10, "lhz never give up!".getBytes()));
        valList.add(new Record(100, "happy every day!".getBytes()));
        valList.add(new Record(400, "no !".getBytes()));

        fileSystem.writeFile(new File(FilePath.toIginxPath("unit0000",path)), valList,tags,false);
    }

    @Test
    public void testReadFile() throws IOException, PhysicalException {
        String path = "a.b.lhz";
        FileSystemImpl fileSystem = new FileSystemImpl();
        AndFilter andFilter = new AndFilter(new ArrayList<Filter>(){{
            add(new KeyFilter(Op.L, 600));
            add(new KeyFilter(Op.GE, 4));
        }});

        Map<String ,String >tags = new HashMap<>();
        tags.put("tt","v1");
        tags.put("tv","v2");
        TagFilter filter = new AndTagFilter(new ArrayList<TagFilter>(){{
            add(new BasePreciseTagFilter(tags));
        }});


        List<FSResultTable> res =fileSystem.readFile(new File(FilePath.toIginxPath("unit0000",path)), filter, andFilter);
        for (FSResultTable record : res) {
            for(Record tmp  : record.getVal()){
                System.out.println(tmp.getRawData());
            }
        }
    }

    @Test
    public void testAppend() throws IOException, PhysicalException {
        String path = "a.b.lhz";
        List<Record> valList = new ArrayList<>();
        FileSystemImpl fileSystem = new FileSystemImpl();
        Map<String, String> tags = new HashMap<>();
        tags.put("tt","v1");
        tags.put("tv","v2");
        valList.add(new Record(50, "middle!".getBytes()));
        valList.add(new Record(500, "end!".getBytes()));
        valList.add(new Record(600, "end2!".getBytes()));

        fileSystem.writeFile(new File(FilePath.toIginxPath("unit0000",path)), valList,tags,false);

        AndFilter andFilter = new AndFilter(new ArrayList<Filter>(){{
            add(new KeyFilter(Op.L, 600));
            add(new KeyFilter(Op.GE, 4));
        }});

        List<FSResultTable> res =fileSystem.readFile(new File(FilePath.toIginxPath("unit0000",path)), null, andFilter);
        for (FSResultTable record : res) {
            for(Record tmp  : record.getVal()){
                System.out.println(tmp.getRawData());
            }
        }
    }

    @Test
    public void testExecuteQueryTask() throws PhysicalException {
        TaskExecuteResult res = null;
        LocalExecutor local = new LocalExecutor();
        AndFilter andFilter = new AndFilter(new ArrayList<Filter>(){{
            add(new KeyFilter(Op.L, 600));
            add(new KeyFilter(Op.GE, 4));
        }});

        Map<String ,String >tags = new HashMap<>();
        tags.put("tt","v1");
        tags.put("tv","v2");
        TagFilter filter = new AndTagFilter(new ArrayList<TagFilter>(){{
            add(new BasePreciseTagFilter(tags));
        }});

        List<String> series = new ArrayList<String >(){{
            add("a.b.lhz");
            add("a.b.lhh");
        }};

        //        String storageUnit, List<String> series, TagFilter tagFilter, Filter filter
        res=local.executeQueryTask("unit0000", series, filter,andFilter);

        RowStream rowStream = res.getRowStream();
        while(rowStream.hasNext()) {
            System.out.println(new String(JsonUtils.toJson(rowStream.next())));
        }
        System.out.println(new String(JsonUtils.toJson(res)));
    }

    @Test
    public void testTrim() throws IOException {
        String path = "a.b.lhz";
        FileSystemImpl fileSystem = new FileSystemImpl();

        Map<String ,String >tags = new HashMap<>();
        tags.put("tt","v1");
        tags.put("tv","v2");
        TagFilter filter = new AndTagFilter(new ArrayList<TagFilter>(){{
            add(new BasePreciseTagFilter(tags));
        }});

        fileSystem.trimFileContent(new File(FilePath.toIginxPath("unit0000",path)),
            filter,
            20,
            100);
    }

    @Test
    public void testDeleteFiles() throws IOException { // 清空文件夹
        String path = "a.b.asd";
        File file = new File(FilePath.toIginxPath("unit0001",null));
        FileSystemImpl fileSystem = new FileSystemImpl();
//        fileSystem.deleteFiles(Collections.singletonList(file));
    }

    @Test
    public void testInsertRowRecords() throws IOException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz2.txt";
        FileSystemImpl fileSystem = new FileSystemImpl();
        List<List<Record>> valList = new ArrayList<>();
        List<File> fileList = new ArrayList<>();
        List<Boolean> ifAppend = new ArrayList<>();

        fileList.add(new File(path));

        valList.add(new ArrayList<Record>() {{
            long key = TimeUtils.MIN_AVAILABLE_TIME;
            add(new Record(key++, "lhz never give up!\n".getBytes()));
            add(new Record(key++, "happy every day!\n".getBytes()));
        }});

        ifAppend.add(false);
        fileSystem.writeFiles(fileList, valList, ifAppend);
    }

    @Test
    public void testGetTimeSeriesOfStorageUnit() throws IOException, PhysicalException {
        LocalExecutor localExecutor = new LocalExecutor();
        List<Timeseries> pathList = localExecutor.getTimeSeriesOfStorageUnit("unit0000");
        for (Timeseries timeseries : pathList) {
            System.out.println(timeseries.getPath());
            System.out.println(timeseries.getTags());
        }
    }

    @Test
    public void testGetBoundaryOfStorage() throws IOException, PhysicalException {
        LocalExecutor localExecutor = new LocalExecutor();
        Pair<TimeSeriesRange, TimeInterval> res = localExecutor.getBoundaryOfStorage(null);
        System.out.println(res.k.getStartTimeSeries());
        System.out.println(res.k.getEndTimeSeries());
        System.out.println(res.v.getStartTime());
        System.out.println(res.v.getEndTime());
    }

    @Test
    public void testReadAndWriteIginxFileByKey() throws IOException {
        String path = "src/test/java/cn.edu.tsinghua.iginx/lhz.iginx.csv";
        FileSystemImpl fileSystem = new FileSystemImpl();
        List<Record> valList = new ArrayList<Record>() {{
            long key = TimeUtils.MIN_AVAILABLE_TIME;
            add(new Record(key++, "Do not go gentle into that good night.".getBytes()));
            add(new Record(key++, "Rage, rage against the dying of the light.".getBytes()));
        }};

        fileSystem.writeFile(new File(path), valList, false);

        List<Record> res = fileSystem.readFile(new File(path), 0, 11);
        for (Record record : res) {
            System.out.println(record.getRawData());
        }
    }

    @Test
    public void testFilterTransformer() {
        Filter keyFilter1 = new KeyFilter(Op.GE, 1000);
        Filter keyFilter2 = new KeyFilter(Op.LE, 2000);
        List<Filter> child = new ArrayList<>();
        child.add(keyFilter1);
        child.add(keyFilter2);

        Filter andFilter = new AndFilter(child);

        byte[] res = FilterTransformer.toBinary(andFilter);

        System.out.println(new String(res));

        Filter filter = FilterTransformer.toFilter(res);
        System.out.println(filter);
    }

    @Test
    public void test() {
        String expression = "(t1==vv1 and t2==vv2) or (t3==v4 and t4==h3)";
        JexlEngine jexl = new JexlBuilder().create();
        JexlExpression jexlExpression = jexl.createExpression(expression);

        JexlContext context = new MapContext();
        context.set("t1", "value1");
        context.set("t2", "value2");
        context.set("t3", "value3");
        context.set("vv1", "value11");
        context.set("vv2", "value2");
        context.set("v4", "value3");

        Boolean result = (Boolean) jexlExpression.evaluate(context);
        System.out.println("Expression result: " + result);
    }

    @Test
    public void testPathConvert(){
        System.out.println(
            FilePath.toIginxPath("unit000000","lhz.tt.rrr##lhz.iginx"));
        System.out.println(
            FilePath.toIginxPath("unit000000","lhz.tt.rrr#lhz.iginx"));
    }

}
