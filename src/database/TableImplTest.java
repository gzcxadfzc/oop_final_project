package database;

import org.junit.jupiter.api.Test;

import java.util.List;

class TableImplTest {
    private final Column column = new ColumnImpl("id", List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column2 = new ColumnImpl("header2", List.of("1", "", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column3 = new ColumnImpl("header3", List.of("1", "2.1", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column4 = new ColumnImpl("header4", List.of("1", "", "a", "b", "5", "6", "7", "8", "9", "10"));
    private final Column column5 = new ColumnImpl("header", List.of("1", "", "3", "2.0", "5", "6", "7", "8", "9", "10"));

    @Test
    void crossJoin() {
    }

    @Test
    void innerJoin() {
    }

    @Test
    void outerJoin() {
    }

    @Test
    void fullOuterJoin() {
    }

    @Test
    void getName() {
    }

    @Test
    void show() {
        TableImpl table = new TableImpl(List.of(column, column2));
        table.show();
    }

    @Test
    void describe() {
        Table table = new TableImpl(List.of(column, column2, column3, column4, column5));
        table.describe();
    }

    @Test
    void head() {
        Table table = new TableImpl(List.of(column, column2, column3, column4, column5));
        table.head().show();
    }

    @Test
    void head2() {
        Table table = new TableImpl(List.of(column, column2, column3, column4, column5));
        table.head(2).show();
    }

    @Test
    void tail() {
        Table table = new TableImpl(List.of(column, column2, column3, column4, column5));
        table.tail().show();
    }

    @Test
    void tail2() {
        Table table = new TableImpl(List.of(column, column2, column3, column4, column5));
        table.tail(2).show();
    }

    @Test
    void selectRows() {
    }

    @Test
    void selectRowsAt() {
    }

    @Test
    void selectColumns() {
    }

    @Test
    void selectColumnsAt() {
    }

    @Test
    void selectRowsBy() {
    }

    @Test
    void sort() {
    }

    @Test
    void getRowCount() {
    }

    @Test
    void getColumnCount() {
    }

    @Test
    void getColumn() {
    }

    @Test
    void testGetColumn() {
    }
}