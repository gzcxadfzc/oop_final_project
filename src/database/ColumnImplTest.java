package database;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
class ColumnImplTest {

    private static final String TEST_NAME = "testName";
    private static final List<String> DATA = List.of("dummy1", "dummy2", "dummy3");
    private static final List<String> INTEGER_DATA = List.of("1", "2", "-1", "0");
    private static final List<String> NUMERIC_DATA = List.of("1.0", "2.010101010", "-1.1", "0");
    private static final List<String> NULL_DATA = List.of("", "2", "-1", "0", "");


    @Test
    void createColumnImpl() {
        Column column = new ColumnImpl(TEST_NAME, NULL_DATA);
    }

    @Test
    void getHeader() {
        Column column = new ColumnImpl(TEST_NAME, NULL_DATA);
        String actual = column.getHeader();
        assertEquals(TEST_NAME, actual);
    }

    @Test
    void getValue() {
        Column column = new ColumnImpl(TEST_NAME, NUMERIC_DATA);
        int actual = column.getValue(1, Integer.class );
        int expected = 2;
        assertEquals(expected, actual);
    }

    @Test
    void testGetValue() {
    }

    @Test
    void setValueString() {
        String expected = "changed";
        int changeIndex = 0;
        Column column = new ColumnImpl(TEST_NAME, DATA);
        column.setValue(changeIndex, expected);
        assertEquals(column.getValue(0), expected);
    }

    @Test
    void setValueInteger() {
        String expected = "99";
        int changeIndex = 0;
        Column column = new ColumnImpl(TEST_NAME, INTEGER_DATA);
        column.setValue(changeIndex, expected);
        assertEquals(column.getValue(0), expected);
    }

    @Test
    void count() {
        Column column = new ColumnImpl(TEST_NAME, NULL_DATA);
        int expected = 5;
        int actual = column.count();
        assertEquals(expected, actual);
    }

    @Test
    void show() {
        Column column = new ColumnImpl(TEST_NAME, NULL_DATA);
        column.show();
    }

    @Test
    void isNumericColumn() {
        Column numericColumn = new ColumnImpl(TEST_NAME, INTEGER_DATA);
        boolean actual = numericColumn.isNumericColumn();
        assertTrue(actual);
    }

    @Test
    void getNullCount() {
        Column column = new ColumnImpl(TEST_NAME, NULL_DATA);
        long expected = 2L;
        long actual = column.getNullCount();
        assertEquals(expected,actual);
    }

}