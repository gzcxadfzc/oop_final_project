package database;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

class TableImplTest {
    private final Column column = new ColumnImpl("id", List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column2 = new ColumnImpl("header2", List.of("1", "", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column3 = new ColumnImpl("header3", List.of("1", "2.1", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final Column column4 = new ColumnImpl("header4", List.of("1", "", "a", "b", "5", "6", "7", "8", "9", "10"));
    private final Column column5 = new ColumnImpl("header", List.of("1", "", "3", "2.0", "5", "6", "7", "8", "9", "10"));
    private final Table table = new TableImpl("table", List.of(column, column2, column3, column4, column5));

    private void createTables() throws FileNotFoundException {
        Database.createTable(new File("rsc/authors.csv"));
        Database.createTable(new File("rsc/editors.csv"));
        Database.createTable(new File("rsc/translators.csv"));
        Database.createTable(new File("rsc/books.csv"));
        Database.createTable(new File("rsc/innerjoin1.csv"));
        Database.createTable(new File("rsc/innerjoin2.csv"));

    }

    @Test
    void crossJoin() throws FileNotFoundException {
        createTables();
        Database.getTable("books")
                .crossJoin(Database.getTable("authors"))
                .show();
    }

    @Test
    void innerJoin() throws FileNotFoundException {
        createTables();
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");

        Table testTable = books;
        Table rightTable = authors;
        Table inner1 = Database.getTable("innerjoin1");
        Table inner2 = Database.getTable("innerjoin2");

        Table innerJoined = testTable.innerJoin(rightTable, List.of(new JoinColumn("author_id", "id")));
        innerJoined.show();
        Table innerJoined2 = inner1.innerJoin(testTable, List.of(new JoinColumn("id", "id"),new JoinColumn("column3", "author_id")));
        innerJoined2.show();
    }

    @Test
    void outerJoin() {
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");

        Table testTable = books;
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
        TableImpl tableImpl = new TableImpl("tableImpl", List.of(column, column2, column3, column4, column5));
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