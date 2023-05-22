package database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TableImpl implements Table {

    private static final String NULL_TEXT = "null";
    private static final String END_LINE = " |";
    private static final String SPACE = " ";
    private static final int NON_NULL_COUNT_CELL_SIZE = 16;
    private static final int DEFAULT_ROW_SIZE = 5;
    private final List<Column> columns;
    private final Map<String, Integer> cellSizes;
    private final int entrySize;

    public TableImpl(List<Column> columns) {
        validateColumnLengths(columns);
        this.columns = columns;
        this.cellSizes = initCellSizes(columns);
        this.entrySize = columns.get(0).count();
    }

    private void validateColumnLengths(List<Column> columns) {
        boolean hasSameLength = columns.stream()
                .map(Column::count)
                .collect(Collectors.toSet())
                .size() == 1;
        if (!hasSameLength) {
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
        for (int i = 0; i < column.count(); i++) {
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
        for (int i = 0; i < tableRow; i++) {
            for (Column column : columns) {
                showNthCell(i, column);
            }
            System.out.println();
        }
    }

    private void showHeaders() {
        for (Column column : columns) {
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
        int entryCount = columns.get(0).count();
        int columnCount = columns.size();
        int columnCellSize = getLongestHeaderCellSize();
        int indexCellSize = columnCount / 10;
        System.out.println("<database.Table@" + this.hashCode() + ">");
        System.out.println("Range Index: " + entryCount + " entries, 0 to " + (entryCount - 1));
        System.out.println("Data Columns (total " + columnCount + " columns) :");
        showDescribeHeader(indexCellSize, columnCellSize);
        showDescribeValues(columnCount, indexCellSize, columnCellSize);

    }

    private int getLongestHeaderCellSize() {
        int max = 0;
        for (Map.Entry<String, Integer> cellSize : cellSizes.entrySet()) {
            max = Math.max(max, cellSize.getValue());
        }
        return max;
    }

    private void showDescribeHeader(int indexCellSize, int columnCellSize) {
        showCell("#", indexCellSize);
        showCell("Column", columnCellSize);
        showCell("Non-Null Count ", NON_NULL_COUNT_CELL_SIZE);
        System.out.println(" Dtype");
    }

    private void showDescribeValues(int columnCount, int indexCellSize, int columnCellSize) {
        for (int i = 0; i < columnCount; i++) {
            showCell(String.valueOf(i), indexCellSize);
            Column column = columns.get(i);
            showCell(column.getHeader(), columnCellSize);
            showCell(column.count() - column.getNullCount() + " non-null", NON_NULL_COUNT_CELL_SIZE);
            System.out.println(getDataType(column));
        }
    }

    private String getDataType(Column column) {
        if (column.isNumericColumn()) {
            return "int";
        }
        return "String";
    }

    @Override
    public Table head() {
        int lineCount = Math.min(columns.size(), DEFAULT_ROW_SIZE);
        return selectRows(0, lineCount);
    }

    @Override
    public Table head(int lineCount) {
        return selectRows(0, lineCount);
    }

    @Override
    public Table tail() {
        int lastIndex = entrySize - 1;
        return selectRows(lastIndex, lastIndex - DEFAULT_ROW_SIZE);
    }

    @Override
    public Table tail(int lineCount) {
        int lastIndex = entrySize - 1;
        return selectRows(lastIndex, lastIndex - lineCount);
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        int indexCount;
        if (beginIndex < endIndex) {
            indexCount = endIndex - beginIndex;
            return new TableImpl(selectColumnsTopDown(beginIndex, indexCount));
        }
        indexCount = beginIndex - endIndex;
        return new TableImpl(selectColumnsBottomUp(beginIndex, indexCount));
    }

    private List<Column> selectColumnsTopDown(int beginIndex, int indexCount) {
        List<Column> copyColumns = new ArrayList<>();
        for (Column column : this.columns) {
            List<String> copyValues = new ArrayList<>();
            for (int i = beginIndex; i < indexCount; i++) {
                copyValues.add(column.getValue(i));
            }
            copyColumns.add(new ColumnImpl(column.getHeader(), copyValues));
        }
        return copyColumns;
    }

    private List<Column> selectColumnsBottomUp(int beginIndex, int indexCount) {
        List<Column> copyColumns = new ArrayList<>();
        for (Column column : this.columns) {
            List<String> copyValues = new ArrayList<>();
            for (int i = 0; i < indexCount; i++) {
                copyValues.add(column.getValue(beginIndex - i));
            }
            copyColumns.add(new ColumnImpl(column.getHeader(), copyValues));
        }
        return copyColumns;
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
}
