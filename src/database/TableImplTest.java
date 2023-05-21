package database;

import org.junit.jupiter.api.Test;

import java.util.List;

class TableImplTest {

    @Test
    void tlqkf() {

    }
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
        Column column = new ColumnImpl("header1", List.of("1000000","2","3","4"));
        Column column2 = new ColumnImpl("header2", List.of("1","","3","4"));
        TableImpl table = new TableImpl(List.of(column, column2), "table");
        table.show();
    }

    @Test
    void describe() {
        Column column = new ColumnImpl("hr1", List.of("1000000","2","3","4"));
        Column column2 = new ColumnImpl("header2", List.of("1","","3","4"));
        Column column3 = new ColumnImpl("header3", List.of("1","2.1","3","4"));
        Column column4 = new ColumnImpl("header4", List.of("1","","3","4"));
        Column column5 = new ColumnImpl("header", List.of("1","","3","4"));

        Table table = new TableImpl(List.of(column, column2, column3, column4, column5), "table");
        table.describe();
    }

    @Test
    void head() {
    }

    @Test
    void testHead() {
    }

    @Test
    void tail() {
    }

    @Test
    void testTail() {
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