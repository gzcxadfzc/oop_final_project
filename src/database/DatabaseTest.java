package database;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {

    @Test
    void showTables() {
    }

    @Test
    void createTable() throws FileNotFoundException {
        Database.createTable(new File("rsc/authors.csv"));
        Database.createTable(new File("rsc/editors.csv"));
        Database.createTable(new File("rsc/translators.csv"));
        Database.createTable(new File("rsc/books.csv"));
        Database.getTable("authors").show();
        Database.getTable("editors").show();
        Database.getTable("translators").show();
        Database.getTable("books").show();
    }

    @Test
    void getTable() {
    }

    @Test
    void sort() throws FileNotFoundException{
        createTable();
        Table testTable = Database.getTable("books");
        Table sortedTable;
        testTable.sort(5, true, false).show();
        sortedTable = testTable.sort(5, true, false);
        System.out.println("identity test for sort(index, asc, nullOrder): " + (!testTable.equals(sortedTable) ? "Fail" : "Pass"));

        Database.sort(testTable, 5, false, true).show();
        sortedTable = Database.sort(testTable, 5, false, true);
        System.out.println("identity test for Database.sort(index, asc, nullOrder): " + (testTable.equals(sortedTable) ? "Fail" : "Pass"));
    }
}