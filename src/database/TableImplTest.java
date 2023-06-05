package database;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

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
        Table innerJoined2 = inner1.innerJoin(testTable, List.of(new JoinColumn("id", "id"),new JoinColumn("column1", "author_id")));
        innerJoined2.show();
    }

    @Test
    void outerJoin() throws FileNotFoundException {
        createTables();
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");

        Table testTable = books;
        Table rightTable = translators;

        Table outerJoined = testTable.outerJoin(rightTable, List.of(new JoinColumn("translator_id", "id")));
        outerJoined.show();
    }

    @Test
    void fullOuterJoin() throws FileNotFoundException {
        createTables();
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");

        Table testTable = books;
        Table rightTable = authors;

        testTable.show();
        rightTable.show();
        Table fullOuterJoined = testTable.fullOuterJoin(rightTable, List.of(new JoinColumn("author_id", "id")));
        fullOuterJoined.show();
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
    void head2() throws FileNotFoundException {
        createTables();
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");
        Table testTable;
        Table headTable;
        testTable = books;
        testTable.head(10).show();
        headTable = testTable.head(10);
        System.out.println("identity test for head(n): " + (testTable.equals(headTable) ? "Fail" : "Pass"));
        testTable.equals(headTable);
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
//        tableImpl.union(tableImpl.selectOneRow(3), tableImpl.selectOneRow(2)).show();
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
    void sort() throws FileNotFoundException {
        createTables();
        Table books = Database.getTable("books");
        Table authors = Database.getTable("authors");
        Table editors = Database.getTable("editors");
        Table translators = Database.getTable("translators");

        Table testTable = books;
        Table rightTable = authors;
        Table sortedTable;
        testTable.sort(5, false, true).show();
        sortedTable = testTable.sort(5, true, false);
        System.out.println("identity test for sort(index, asc, nullOrder): " + (!testTable.equals(sortedTable) ? "Fail" : "Pass"));

        Database.sort(testTable, 5, false, true).show();
        sortedTable = Database.sort(testTable, 5, false, true);
        System.out.println("identity test for Database.sort(index, asc, nullOrder): " + (testTable.equals(sortedTable) ? "Fail" : "Pass"));
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

    @Test
    void testEquals() {
        Column c1 = new ColumnImpl("any1", List.of("1","1"));
        Column c2 = new ColumnImpl("any", List.of("1","1"));

        Table table3 = new TableImpl("a", List.of(c1, c2));
        System.out.println(table3.selectRowsAt(0));
        System.out.println(table3.selectRowsAt(1));
        assertEquals(table3.selectRowsAt(0), table3.selectRowsAt(1));
        Set<Table> tables = new HashSet<>();
        tables.add(table3.selectRowsAt(0));
        tables.add(table3.selectRowsAt(1));
        System.out.println(tables);
    }
}