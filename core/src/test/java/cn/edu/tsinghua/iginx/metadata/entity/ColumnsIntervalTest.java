package cn.edu.tsinghua.iginx.metadata.entity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnsIntervalTest {
  public static final Logger logger = LoggerFactory.getLogger(ColumnsIntervalTest.class);
  private ColumnsInterval columnsInterval;
  private String[] arrayName = {"null", "a.a", "b.b", "c.c", "d.d", "null"};

  @Before
  public void setUp() {
    columnsInterval = new ColumnsInterval("col1", "col5", true);
  }

  @Test
  public void testGetStartColumn() {
    assertEquals("col1", columnsInterval.getStartColumn());
  }

  @Test
  public void testSetStartColumn() {
    columnsInterval.setStartColumn("col2");
    assertEquals("col2", columnsInterval.getStartColumn());
  }

  @Test
  public void testGetEndColumn() {
    assertEquals("col5", columnsInterval.getEndColumn());
  }

  @Test
  public void testSetEndColumn() {
    columnsInterval.setEndColumn("col6");
    assertEquals("col6", columnsInterval.getEndColumn());
  }

  @Test
  public void testIsClosed() {
    assertTrue(columnsInterval.isClosed());
  }

  @Test
  public void testSetClosed() {
    columnsInterval.setClosed(false);
    assertFalse(columnsInterval.isClosed());
  }

  @Test
  public void testToString() {
    assertEquals("col1-col5", columnsInterval.toString());
  }

  @Test
  public void testSetSchemaPrefix() {
    columnsInterval.setSchemaPrefix("schema");
    assertEquals("schema", columnsInterval.getSchemaPrefix());
  }

  @Test
  public void testEquals() {
    ColumnsInterval other = new ColumnsInterval("col1", "col5", true);
    assertTrue(columnsInterval.equals(other));
  }

  @Test
  public void testHashCode() {
    ColumnsInterval other = new ColumnsInterval("col1", "col5", true);
    assertEquals(columnsInterval.hashCode(), other.hashCode());
  }

  @Test
  public void testCompareTo() {
    ColumnsInterval other = new ColumnsInterval("col1", "col5", true);
    assertEquals(0, columnsInterval.compareTo(other));

    other = new ColumnsInterval("col2", "col6", true);
    assertEquals(-1, columnsInterval.compareTo(other));

    other = new ColumnsInterval("col0", "col4", false);
    assertEquals(1, columnsInterval.compareTo(other));
  }

  @Test
  public void testIsCompletelyAfter() {
    List<Interval> intervals = getIntervals(Arrays.asList(0, 1, 2, 3, 4, 5));
    for (int i = 0; i < intervals.size(); i++) {
      for (int j = i; j < intervals.size(); j++) {
        Interval intervalA = intervals.get(i);
        Interval intervalB = intervals.get(j);
        boolean res = testIsCompletelyAfter(intervalA, intervalB);
        ColumnsInterval ca = getColumnRangeFromInterval(intervalA);
        ColumnsInterval cb = getColumnRangeFromInterval(intervalB);
        if (res != ca.isCompletelyAfter(cb)) {
          logger.error(
              "expect {} but was {}, with {}, {} ",
              res,
              ca.isCompletelyAfter(cb),
              intervalA.toString(),
              intervalB.toString());
          fail();
        }
      }
    }
  }

  @Test
  public void testIsIntersect() {
    List<Interval> intervals = getIntervals(Arrays.asList(0, 1, 2, 3, 4, 5));
    for (int i = 0; i < intervals.size(); i++) {
      for (int j = i; j < intervals.size(); j++) {
        Interval intervalA = intervals.get(i);
        Interval intervalB = intervals.get(j);
        boolean res = testIsIntersect(intervalA, intervalB);
        ColumnsInterval ca = getColumnRangeFromInterval(intervalA);
        ColumnsInterval cb = getColumnRangeFromInterval(intervalB);
        if (res != ca.isIntersect(cb)) {
          logger.error(
              "expect {} but was {}, with {}, {} ",
              res,
              ca.isIntersect(cb),
              intervalA.toString(),
              intervalB.toString());
          fail();
        }
      }
    }
  }

  @Test
  public void testIsContain() {
    List<Integer> list = Arrays.asList(0, 1, 2, 3, 4, 5);
    List<Interval> intervals = getIntervals(list);

    for (int i = 0; i < intervals.size(); i++) {
      for (int j = 1; j < list.size() - 1; j++) {
        Interval intervalA = intervals.get(i);
        boolean res = testIsContain(intervalA, list.get(j));
        ColumnsInterval ca = getColumnRangeFromInterval(intervalA);
        if (res != ca.isContain(arrayName[j])) {
          logger.error(
              "expect {} but was {}, with {}, {} ",
              res,
              ca.isContain(arrayName[j]),
              intervalA.toString(),
              arrayName[j]);
          fail();
        }
      }
    }
  }

  private ColumnsInterval getColumnRangeFromInterval(Interval interval) {
    int start = interval.start;
    int end = interval.end;
    boolean isClosed = interval.isClosed;
    return new ColumnsInterval(getNameFromIndex(start), getNameFromIndex(end), isClosed);
  }

  private String getNameFromIndex(int index) {
    if (arrayName[index].equals("null")) {
      return null;
    } else {
      return arrayName[index];
    }
  }

  public List<Interval> getIntervals(List<Integer> numbers) {
    List<Interval> intervals = new ArrayList<>();
    for (int i = 0; i < numbers.size(); i++) {
      for (int j = i; j < numbers.size(); j++) {
        boolean isClosed = new Random().nextBoolean();
        if (i == 0 && j == 0 || i == numbers.size() - 1 && j == numbers.size() - 1) continue;
        intervals.add(new Interval(numbers.get(i), numbers.get(j), i == j ? true : isClosed));
      }
    }
    return intervals;
  }

  private boolean testIsCompletelyAfter(Interval a, Interval b) {
    if (b.isClosed) {
      return a.start > b.end;
    } else {
      return a.start >= b.end;
    }
  }

  public static boolean testIsIntersect(Interval a, Interval b) {
    if (a.isClosed && b.isClosed) {
      return !(a.end < b.start || a.start > b.end);
    } else if (a.isClosed) {
      return !(a.end < b.start || a.start >= b.end);
    } else if (b.isClosed) {
      return !(a.end <= b.start || a.start > b.end);
    } else {
      return !(a.end <= b.start || a.start >= b.end);
    }
  }

  private boolean testIsContain(Interval a, int b) {
    if (a.isClosed) {
      return a.start <= b && a.end >= b;
    } else {
      return a.start <= b && a.end > b;
    }
  }

  class Interval {
    int start;
    int end;
    boolean isClosed;

    public Interval(int start, int end, boolean isClosed) {
      this.start = start;
      this.end = end;
      this.isClosed = isClosed;
    }

    @Override
    public String toString() {
      return "Interval [start=" + start + ", end=" + end + ", isClosed=" + isClosed + "]";
    }
  }
}
