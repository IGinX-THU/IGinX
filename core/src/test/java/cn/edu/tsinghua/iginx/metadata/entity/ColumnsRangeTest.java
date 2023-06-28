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

public class ColumnsRangeTest {
  public static final Logger logger = LoggerFactory.getLogger(ColumnsRangeTest.class);
  private ColumnsRange columnsRange;
  private String[] arrayName = {"null", "a.a", "b.b", "c.c", "d.d", "null"};

  @Before
  public void setUp() {
    columnsRange = new ColumnsRange("col1", "col5", true);
  }

  @Test
  public void testGetStartColumn() {
    assertEquals("col1", columnsRange.getStartColumn());
  }

  @Test
  public void testSetStartColumn() {
    columnsRange.setStartColumn("col2");
    assertEquals("col2", columnsRange.getStartColumn());
  }

  @Test
  public void testGetEndColumn() {
    assertEquals("col5", columnsRange.getEndColumn());
  }

  @Test
  public void testSetEndColumn() {
    columnsRange.setEndColumn("col6");
    assertEquals("col6", columnsRange.getEndColumn());
  }

  @Test
  public void testIsClosed() {
    assertTrue(columnsRange.isClosed());
  }

  @Test
  public void testSetClosed() {
    columnsRange.setClosed(false);
    assertFalse(columnsRange.isClosed());
  }

  @Test
  public void testToString() {
    assertEquals("col1-col5", columnsRange.toString());
  }

  @Test
  public void testSetSchemaPrefix() {
    columnsRange.setSchemaPrefix("schema");
    assertEquals("schema", columnsRange.getSchemaPrefix());
  }

  @Test
  public void testEquals() {
    ColumnsRange other = new ColumnsRange("col1", "col5", true);
    assertTrue(columnsRange.equals(other));
  }

  @Test
  public void testHashCode() {
    ColumnsRange other = new ColumnsRange("col1", "col5", true);
    assertEquals(columnsRange.hashCode(), other.hashCode());
  }

  @Test
  public void testCompareTo() {
    ColumnsRange other = new ColumnsRange("col1", "col5", true);
    assertEquals(0, columnsRange.compareTo(other));

    other = new ColumnsRange("col2", "col6", true);
    assertEquals(-1, columnsRange.compareTo(other));

    other = new ColumnsRange("col0", "col4", false);
    assertEquals(1, columnsRange.compareTo(other));
  }

  @Test
  public void testIsCompletelyAfter() {
    List<Interval> intervals = getIntervals(Arrays.asList(0, 1, 2, 3, 4, 5));
    for (int i = 0; i < intervals.size(); i++) {
      for (int j = i; j < intervals.size(); j++) {
        Interval intervalA = intervals.get(i);
        Interval intervalB = intervals.get(j);
        boolean res = testIsCompletelyAfter(intervalA, intervalB);
        ColumnsRange ca = getColumnRangeFromInterval(intervalA);
        ColumnsRange cb = getColumnRangeFromInterval(intervalB);
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
        ColumnsRange ca = getColumnRangeFromInterval(intervalA);
        ColumnsRange cb = getColumnRangeFromInterval(intervalB);
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
        ColumnsRange ca = getColumnRangeFromInterval(intervalA);
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

  private ColumnsRange getColumnRangeFromInterval(Interval interval) {
    int start = interval.start;
    int end = interval.end;
    boolean isClosed = interval.isClosed;
    return new ColumnsRange(getNameFromIndex(start), getNameFromIndex(end), isClosed);
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
