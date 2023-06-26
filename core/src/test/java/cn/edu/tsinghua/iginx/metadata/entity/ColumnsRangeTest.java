package cn.edu.tsinghua.iginx.metadata.entity;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ColumnsRangeTest {

  private ColumnsRange columnsRange;

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
  public void testIsContain() {
    assertTrue(columnsRange.isContain("col1"));
    assertTrue(columnsRange.isContain("col3"));
    assertTrue(columnsRange.isContain("col5"));
    assertFalse(columnsRange.isContain("col0"));
    assertFalse(columnsRange.isContain("col6"));
  }

  @Test
  public void testIsCompletelyBefore() {
    assertFalse(columnsRange.isCompletelyBefore("col1"));
    assertFalse(columnsRange.isCompletelyBefore("col3"));
    assertTrue(columnsRange.isCompletelyBefore("col6"));
  }

  @Test
  public void testIsIntersect() {
    ColumnsRange other = new ColumnsRange("col3", "col7", true);
    assertTrue(columnsRange.isIntersect(other));

    other = new ColumnsRange("col6", "col7", false);
    assertFalse(columnsRange.isIntersect(other));
  }

  @Test
  public void testGetIntersect() {
    ColumnsRange other = new ColumnsRange("col3", "col7", true);
    ColumnsRange intersect = columnsRange.getIntersect(other);
    assertNotNull(intersect);
    assertEquals("col3", intersect.getStartColumn());
    assertEquals("col5", intersect.getEndColumn());

    other = new ColumnsRange("col6", "col7", false);
    intersect = columnsRange.getIntersect(other);
    assertNull(intersect);
  }

  @Test
  public void testIsCompletelyAfter() {
    ColumnsRange other = new ColumnsRange("col0");
    assertTrue(columnsRange.isCompletelyAfter(other));

    other = new ColumnsRange("col0", "col3", true);
    assertFalse(columnsRange.isCompletelyAfter(other));
  }

  @Test
  public void testIsAfter() {
    assertTrue(columnsRange.isAfter("col0"));
    assertFalse(columnsRange.isAfter("col1"));
    assertFalse(columnsRange.isAfter("col3"));
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
  public void testFromString() {
    String input = "col1-col5";
    ColumnsRange result = ColumnsRange.fromString(input);
    assertNotNull(result);
    assertEquals("col1", result.getStartColumn());
    assertEquals("col5", result.getEndColumn());
    assertFalse(result.isClosed());

    input = "col1.*";
    result = ColumnsRange.fromString(input);
    assertNotNull(result);
    assertEquals("col1.*", result.getStartColumn());
    assertEquals("col1.*", result.getEndColumn());
    assertTrue(result.isClosed());
  }
}
