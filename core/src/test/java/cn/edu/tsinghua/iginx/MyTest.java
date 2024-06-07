package cn.edu.tsinghua.iginx;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MyTest {

  private static Random random = new Random();

  ForkJoinPool pool = new ForkJoinPool(5);
  ExecutorService executor = Executors.newFixedThreadPool(5);

  @org.junit.Test
  public void testA() throws ExecutionException, InterruptedException {
    //    Operator before = TreeBuilder.buildFilterFragmentTree();
    //    String a = FastjsonSerializeUtils.serialize(before);
    //    Operator after = FastjsonSerializeUtils.deserialize(a, Operator.class);
    int size = 1000_0000;
    List<Row> rows = getRandomRows(size);

    long startTime, endTime;

    startTime = System.currentTimeMillis();
    Map<Integer, List<Row>> map1 = new HashMap<>();
    for (Row row : rows) {
      int hash = row.index;
      List<Row> l = map1.computeIfAbsent(hash, k -> new ArrayList<>());
      l.add(row);
    }
    endTime = System.currentTimeMillis();
    System.out.println("sequence cost time: " + (endTime - startTime));

    startTime = System.currentTimeMillis();
    Map<Integer, List<Row>> map2 =
        pool.submit(
                () ->
                    rows.parallelStream()
                        .collect(
                            Collectors.groupingBy(
                                row -> {
                                  return row.index;
                                })))
            .get();
    endTime = System.currentTimeMillis();
    System.out.println("parallel cost time: " + (endTime - startTime));

    startTime = System.currentTimeMillis();
    int range = size / 5;
    Map<Integer, List<Row>> map3 = new HashMap<>();
    List<Map<Integer, List<Row>>> list = new ArrayList<>();

    CountDownLatch latch = new CountDownLatch(5);
    for (int i = 0; i < 5; i++) {
      int finalI = i;
      list.add(new HashMap<>());
      pool.submit(
          () -> {
            for (int j = finalI * range; j < (finalI + 1) * range; j++) {
              Row row = rows.get(j);
              int hash = row.index;
              List<Row> l = list.get(finalI).computeIfAbsent(hash, k -> new ArrayList<>());
              l.add(row);
            }
            latch.countDown();
          });
    }
    latch.await();
    //    list.forEach(map ->
    //        map.forEach((key, value) ->
    //            map3.merge(key, value, (list1, list2) -> {
    //              list1.addAll(list2); // 合并两个列表
    //              return list1;
    //            })
    //        )
    //    );
    Map<Integer, List<Row>> mergedMap =
        pool.submit(
                () ->
                    list.parallelStream()
                        .flatMap(map -> map.entrySet().stream()) // 将每个Map转换为流
                        .collect(
                            Collectors.toConcurrentMap(
                                Map.Entry::getKey, // 键
                                Map.Entry::getValue, // 值
                                (list1, list2) -> { // 合并函数，用于处理重复键
                                  list1.addAll(list2); // 合并两个列表
                                  return list1;
                                })))
            .get();
    endTime = System.currentTimeMillis();
    System.out.println("new way cost time: " + (endTime - startTime));

    assertEquals(map1.size(), map2.size());
    assertEquals(map2.size(), mergedMap.size());
  }

  private static List<Row> getRandomRows(int size) {
    int range = size / 10;
    List<Row> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      result.add(new Row(random.nextInt(range), random.nextInt(size)));
    }
    return result;
  }

  static class Row {
    int index;
    int value;

    public Row(int index, int value) {
      this.index = index;
      this.value = value;
    }
  }
}
