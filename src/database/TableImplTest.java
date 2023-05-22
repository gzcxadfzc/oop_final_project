package database;

import org.junit.jupiter.api.Test;

import java.util.List;

class TableImplTest {
    private final Column column = new ColumnImpl("id", List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column2 = new ColumnImpl("header2", List.of("1", "", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column3 = new ColumnImpl("header3", List.of("1", "2.1", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column4 = new ColumnImpl("header4", List.of("1", "", "a", "b", "5", "6", "7", "8", "9", "10"));
    private final Column column5 = new ColumnImpl("header", List.of("1", "", "3", "2.0", "5", "6", "7", "8", "9", "10"));
    private final Table table = new TableImpl(List.of(column, column2, column3, column4, column5));

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
        table.show();
    }

    @Test
    void describe() {
        table.describe();
    }

    @Test
    void head() {
        table.head().show();
    }

    @Test
    void head2() {
        table.head(2).show();
    }

    @Test
    void tail() {
        table.tail().show();
    }

    @Test
    void tail2() {
        TableImpl tableImpl = new TableImpl(List.of(column, column2, column3, column4, column5));
        table.tail(2).show();
/*        tableImpl.selectOneRow(3).show();
        tableImpl.selectOneRow(2).show();
        tableImpl.concatColumn(column2, column2).show();*/
        tableImpl.union(tableImpl.selectOneRow(3), tableImpl.selectOneRow(2)).show();
    }

    @Test
    void selectRows() {
        table.selectRows(2, 1).show();
    }

    @Test
    void selectRowsAt() {
        table.selectRowsAt(1, 2, 5, 6).show();
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
        table.getColumn("id").show();
    }

    @Test
    void testGetColumn() {
    }
}