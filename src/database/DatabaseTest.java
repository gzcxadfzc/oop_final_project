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
        Database.getTable("authors.csv").show();
        Database.getTable("editors.csv").show();
        Database.getTable("translators.csv").show();
        Database.getTable("books.csv").show();
    }

    @Test
    void getTable() {
    }

    @Test
    void sort() {
    }
}