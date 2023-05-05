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
import cn.edu.tsinghua.iginx.filesystem.filesystem.FileSystemService;
import cn.edu.tsinghua.iginx.filesystem.query.FSResultTable;
import cn.edu.tsinghua.iginx.filesystem.tools.ConfLoader;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.options.ExprOptions;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleList;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.bpodgursky.jbool_expressions.util.ExprFactory;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.jexl3.*;
import org.junit.Test;


public class FuncUT {
    private String root = ConfLoader.getRootPath();

    @Test
    public void testCreateFile() throws IOException, PhysicalException {
        String path = "a.b.lhz";
        List<Record> valList = new ArrayList<>();
        FileSystemService fileSystem = new FileSystemService();
        Map<String, String> tags = new HashMap<>();
        tags.put("tt", "v1");
        tags.put("tv", "v2");
        valList.add(new Record(0, "no !".getBytes()));
        valList.add(new Record(10, "lhz never give up!".getBytes()));
        valList.add(new Record(100, "happy every day!".getBytes()));
        valList.add(new Record(400, "no !".getBytes()));

        fileSystem.writeFile(new File(FilePath.toIginxPath(root, "unit0000", path)), valList, tags);
    }

    @Test
    public void testReadFile() throws IOException, PhysicalException {
        String path = "a.b.lhz";
        FileSystemService fileSystem = new FileSystemService();
        AndFilter andFilter =
                new AndFilter(
                        new ArrayList<Filter>() {
                            {
                                add(new KeyFilter(Op.L, 600));
                                add(new KeyFilter(Op.GE, 4));
                            }
                        });

        Map<String, String> tags = new HashMap<>();
        tags.put("tt", "v1");
        tags.put("tv", "v2");
        TagFilter filter =
                new AndTagFilter(
                        new ArrayList<TagFilter>() {
                            {
                                add(new BasePreciseTagFilter(tags));
                            }
                        });

        List<FSResultTable> res =
                fileSystem.readFile(
                        new File(FilePath.toIginxPath(root, "unit0000", path)), filter, andFilter);
        for (FSResultTable record : res) {
            for (Record tmp : record.getVal()) {
                System.out.println(tmp.getRawData());
            }
        }
    }

    @Test
    public void testAppend() throws IOException, PhysicalException {
        String path = "a.b.lhz";
        List<Record> valList = new ArrayList<>();
        FileSystemService fileSystem = new FileSystemService();
        Map<String, String> tags = new HashMap<>();
        tags.put("tt", "v1");
        tags.put("tv", "v2");
        valList.add(new Record(50, "middle!".getBytes()));
        valList.add(new Record(500, "end!".getBytes()));
        valList.add(new Record(600, "end2!".getBytes()));

        fileSystem.writeFile(new File(FilePath.toIginxPath(root, "unit0000", path)), valList, tags);

        AndFilter andFilter =
                new AndFilter(
                        new ArrayList<Filter>() {
                            {
                                add(new KeyFilter(Op.L, 600));
                                add(new KeyFilter(Op.GE, 4));
                            }
                        });

        List<FSResultTable> res =
                fileSystem.readFile(
                        new File(FilePath.toIginxPath(root, "unit0000", path)), null, andFilter);
        for (FSResultTable record : res) {
            for (Record tmp : record.getVal()) {
                System.out.println(tmp.getRawData());
            }
        }
    }

    @Test
    public void testExecuteQueryTask() throws PhysicalException {
        TaskExecuteResult res = null;
        LocalExecutor local = new LocalExecutor();
        AndFilter andFilter =
                new AndFilter(
                        new ArrayList<Filter>() {
                            {
                                add(new KeyFilter(Op.L, 600));
                                add(new KeyFilter(Op.GE, 4));
                            }
                        });

        Map<String, String> tags = new HashMap<>();
        tags.put("tt", "v1");
        tags.put("tv", "v2");
        TagFilter filter =
                new AndTagFilter(
                        new ArrayList<TagFilter>() {
                            {
                                add(new BasePreciseTagFilter(tags));
                            }
                        });

        List<String> series =
                new ArrayList<String>() {
                    {
                        add("a.b.lhz");
                        add("a.b.lhh");
                    }
                };

        //        String storageUnit, List<String> series, TagFilter tagFilter, Filter filter
        res = local.executeQueryTask("unit0000", series, filter, andFilter);

        RowStream rowStream = res.getRowStream();
        while (rowStream.hasNext()) {
            System.out.println(new String(JsonUtils.toJson(rowStream.next())));
        }
        System.out.println(new String(JsonUtils.toJson(res)));
    }

    @Test
    public void testTrim() throws IOException {
        String path = "a.b.lhz";
        FileSystemService fileSystem = new FileSystemService();

        Map<String, String> tags = new HashMap<>();
        tags.put("tt", "v1");
        tags.put("tv", "v2");
        TagFilter filter =
                new AndTagFilter(
                        new ArrayList<TagFilter>() {
                            {
                                add(new BasePreciseTagFilter(tags));
                            }
                        });

        fileSystem.trimFileContent(
                new File(FilePath.toIginxPath(root, "unit0000", path)), filter, 20, 100);
    }

    @Test
    public void testDeleteFiles() throws IOException { // 清空文件夹
        String path = "a.b.asd";
        File file = new File(FilePath.toIginxPath(root, "unit0001", null));
        FileSystemService fileSystem = new FileSystemService();
        //        fileSystem.deleteFiles(Collections.singletonList(file));
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
    public void testExpression() {
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
    public void testFilterToString(){
        Filter keyFilter1 = new KeyFilter(Op.G, 1000);
        Filter keyFilter2 = new KeyFilter(Op.LE, 2000);
        List<Filter> child = new ArrayList<>();
        child.add(keyFilter1);
        child.add(keyFilter2);

        Filter filter = new AndFilter(child);
        FilterTransformer filterTransformer = new FilterTransformer();
        BiMap<String, String> vals = HashBiMap.create();
        String res = FilterTransformer.toString(filter, vals);
        Expression<String> nonStandard = ExprParser.parse(res);
        System.out.println(nonStandard.toString());
        System.out.println(vals);
        com.bpodgursky.jbool_expressions.Expression<String> expr = ExprParser.parse("( ( (! C) | C) & b2 & a1)");
        Expression<String> simplified = RuleSet.simplify(expr);
        System.out.println(simplified);
    }

    @Test
    public void testPathConvert() {
        System.out.println(FilePath.toIginxPath(root, "unit000000", "lhz.tt.rrr##lhz.iginx"));
        System.out.println(FilePath.toIginxPath(root, "unit000000", "lhz.tt.rrr#lhz.iginx"));
    }

    @Test
    public void testGetRoot() {
        System.out.println(ConfLoader.getRootPath());
    }
}
