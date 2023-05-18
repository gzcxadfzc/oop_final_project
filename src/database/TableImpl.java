package database;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TableImpl implements Table{

    private static final String NULL_TEXT = "null";
    private static final String END_LINE = " |";
    private static final String SPACE = " ";
    private final List<Column> columns;
    private final String tableName;
    private final Map<String, Integer> cellSizes;
    public TableImpl(List<Column> columns, String tableName) {
        validateColumnLengths(columns);
        this.columns = columns;
        this.tableName = tableName;
        this.cellSizes = initCellSizes(columns);
    }

    private void validateColumnLengths(List<Column> columns) {
        boolean hasSameLength =columns.stream()
                .map(Column::count)
                .collect(Collectors.toSet())
                .size() == 1;
        if(!hasSameLength) {
            throw new IllegalArgumentException("columns have diff length");
        }
    }

    private Map<String, Integer> initCellSizes(List<Column> columns) {
        return columns.stream()
                .collect(Collectors.toMap(Column::getHeader, this::findLongestLength));
    }

    private int findLongestLength(Column column) {
        int headerLength = column.getHeader().length();
        int longestCellLength = 0;
        for(int i = 0; i < column.count(); i ++) {
            longestCellLength = Math.max(longestCellLength, column.getValue(i).length());
        }
        return Math.max(headerLength, longestCellLength);
    }

    @Override
    public Table crossJoin(Table rightTable) {
        return null;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void show() {
        int tableRow = columns.get(0).count();
        showHeaders();
        for(int i = 0; i < tableRow; i ++) {
            for(Column column : columns) {
                showNthCell(i,column);
            }
            System.out.println();
        }
    }

    private void showHeaders() {
        for(Column column : columns) {
            showCell(column.getHeader(), cellSizes.get(column.getHeader()));
        }
        System.out.println();
    }

    private void showNthCell(int index, Column column) {
        String value = column.getValue(index);
        int cellSize = cellSizes.get(column.getHeader());
        showCell(value, cellSize);
    }

    private void showCell(String value, int cellSize) {
        StringBuilder cellText = new StringBuilder();
        String printValue = convertNull(value);
        cellText.append(SPACE.repeat(Math.max(0, cellSize - printValue.length())));
        cellText.append(printValue);
        cellText.append(END_LINE);
        System.out.print(cellText);
    }

    private static String convertNull(String value) {
        if (value.equals("")) {
            return NULL_TEXT;
        }
        return value;
    }

    @Override
    public void describe() {
        System.out.println(super.getClass()); // ???
        System.out.println();
    }

    @Override
    public Table head() {
        return null;
    }

    @Override
    public Table head(int lineCount) {
        return null;
    }

    @Override
    public Table tail() {
        return null;
    }

    @Override
    public Table tail(int lineCount) {
        return null;
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectRowsAt(int... indices) {
        return null;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        return null;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return null;
    }

    @Override
    public int getRowCount() {
        return 0;
    }

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public Column getColumn(int index) {
        return null;
    }

    @Override
    public Column getColumn(String name) {
        return null;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
