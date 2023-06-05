package database;

import java.util.*;
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
    private final String tableName;
    private final int entrySize;
    private final int numericColumnCount;

    public TableImpl(String tableName, List<Column> columns) {
        validateColumnLengths(columns);
        this.tableName = tableName;
        this.columns = columns;
        this.cellSizes = initCellSizes(columns);
        this.entrySize = columns.get(0).count();
        this.numericColumnCount = getNumericColumnCount(columns);
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

    private int getNumericColumnCount(List<Column> columns) {
        return (int) columns.stream()
                .map(Column::isNumericColumn)
                .filter(isNumeric -> isNumeric)
                .count();
    }

    @Override
    public Table crossJoin(Table rightTable) {
        List<Column> newColumns = new ArrayList<>();
        //left table column
        for (Column column : this.columns) {
            String newHeader = tableName + "." + column.getHeader();
            List<String> newColumnValues = new ArrayList<>();
            for (int i = 0; i < column.count(); i++) {
                for (int j = 0; j < rightTable.getRowCount(); j++) {
                    newColumnValues.add(column.getValue(i));
                }
            }
            newColumns.add(new ColumnImpl(newHeader, newColumnValues));
        }
        //right table column
        List<Column> rightTableColumns = copyAnotherTableColumns(rightTable);
        for (Column column : rightTableColumns) {
            String newHeader = rightTable.getName() + "." + column.getHeader();
            Column newColumn = new ColumnImpl(newHeader, copyColumnValues(column));
            for (int i = 1; i < this.getRowCount(); i++) {
                newColumn = concatColumn(newHeader, newColumn, column);
            }
            newColumns.add(newColumn);
        }
        return new TableImpl(this.tableName + rightTable.getName(), newColumns);
    }

    List<Column> copyAnotherTableColumns(Table another) {
        List<Column> copyColumns = new ArrayList<>();
        for (int i = 0; i < another.getColumnCount(); i++) {
            copyColumns.add(another.getColumn(i));
        }
        return copyColumns;
    }

    List<String> copyColumnValues(Column column) {
        List<String> copyValues = new ArrayList<>();
        for (int i = 0; i < column.count(); i++) {
            copyValues.add(column.getValue(i));
        }
        return copyValues;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        List<Table> innerJoinRows = getMatchRows(rightTable, joinColumns);
        if (!innerJoinRows.isEmpty()) {
            return unionTables(innerJoinRows);
        }
        Table emptyRowLeft = extractEmptyRow(this);
        Table emptyRowRight = extractEmptyRow(rightTable);
        return emptyRowLeft.crossJoin(emptyRowRight);
    }

    private List<Table> getMatchRows(Table rightTable, List<JoinColumn> joinColumns) {
        List<Table> innerJoinRows = new ArrayList<>();
        for (int leftColumnIndex = 0; leftColumnIndex < entrySize; leftColumnIndex++) {
            for (int rightColumnIndex = 0; rightColumnIndex < rightTable.getRowCount(); rightColumnIndex++) {
                boolean isMatch = true;
                for (JoinColumn joinColumn : joinColumns) {
                    Column leftColumn = this.getColumn(joinColumn.getColumnOfThisTable());
                    Column rightColumn = rightTable.getColumn(joinColumn.getColumnOfAnotherTable());
                    String value = leftColumn.getValue(leftColumnIndex);
                    if (!value.equals(rightColumn.getValue(rightColumnIndex))) {
                        isMatch = false;
                    }
                }
                if (isMatch) {
                    Table leftRow = this.selectRowsAt(leftColumnIndex);
                    Table rightRow = rightTable.selectRowsAt(rightColumnIndex);
                    innerJoinRows.add(leftRow.crossJoin(rightRow));
                }
            }
        }
        return innerJoinRows;
    }

    private Table extractEmptyRow(Table table) {
        Table row = table.selectRowsAt(0);
        for (int i = 0; i < row.getColumnCount(); i++) {
            row.getColumn(i).setValue(0, "");
        }
        return row;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        List<Table> outerJoinRows = getOuterJoinRows(rightTable, joinColumns);
        return unionTables(outerJoinRows);
    }

    private List<Table> getOuterJoinRows(Table rightTable, List<JoinColumn> joinColumns) {
        List<Table> matchRows = new ArrayList<>();
        List<Table> noMatchRows = new ArrayList<>();
        for (int leftColumnIndex = 0; leftColumnIndex < entrySize; leftColumnIndex++) {
            boolean hasMatchInRightColumn = false;
            for (int rightColumnIndex = 0; rightColumnIndex < rightTable.getRowCount(); rightColumnIndex++) {
                boolean isAllMatchInJoinColumns = true;
                for (JoinColumn joinColumn : joinColumns) {
                    Column leftColumn = this.getColumn(joinColumn.getColumnOfThisTable());
                    Column rightColumn = rightTable.getColumn(joinColumn.getColumnOfAnotherTable());
                    String value = leftColumn.getValue(leftColumnIndex);
                    if (!value.equals(rightColumn.getValue(rightColumnIndex))) {
                        isAllMatchInJoinColumns = false;
                    }
                }
                if (isAllMatchInJoinColumns) {
                    Table leftRow = this.selectRowsAt(leftColumnIndex);
                    Table rightRow = rightTable.selectRowsAt(rightColumnIndex);
                    matchRows.add(leftRow.crossJoin(rightRow));
                    hasMatchInRightColumn = true;
                }
            }
            if (!hasMatchInRightColumn) {
                Table leftRow = this.selectRowsAt(leftColumnIndex);
                Table rightRow = extractEmptyRow(rightTable);
                noMatchRows.add(leftRow.crossJoin(rightRow));
            }
        }
        matchRows.addAll(noMatchRows);
        return matchRows;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        List<Table> tables = new ArrayList<>();
        int innerJoinRowCount = this.innerJoin(rightTable, joinColumns).getRowCount();
        tables.add(this.outerJoin(rightTable, joinColumns));
        List<JoinColumn> swappedJoinColumns = joinColumns.stream()
                .map(JoinColumn::getSwoppedJoinColumn)
                .collect(Collectors.toList());
        Table rightOuterJoin = rightTable.outerJoin(this, swappedJoinColumns);
        rightOuterJoin = reverseColumn(rightOuterJoin);
        if (innerJoinRowCount < rightOuterJoin.getRowCount()) {
            Table slicedRightOuterJoin = rightOuterJoin.selectRows(innerJoinRowCount, rightOuterJoin.getRowCount());
            tables.add(slicedRightOuterJoin);
        }
        Table unionTable = unionTables(tables);
        return removeOverlap(unionTable);
    }

    private Table reverseColumn(Table table) {
        List<Column> reverseColumns = new ArrayList<>();
        for (int i = table.getColumnCount() - 1; i >= 0; i--) {
            reverseColumns.add(table.getColumn(i));
        }
        return new TableImpl(table.getName(), reverseColumns);
    }

    private Table removeOverlap(Table table) {
        Set<Table> tables = new LinkedHashSet<>();
        for (int i = 0; i < table.getRowCount(); i++) {
            tables.add(table.selectRowsAt(i));
        }
        return unionTables(new ArrayList<>(tables));
    }

    @Override
    public String getName() {
        return this.tableName;
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
        System.out.println(this);
        System.out.println(this.hashCode());
        System.out.println("Range Index: " + entryCount + " entries, 0 to " + (entryCount - 1));
        System.out.println("Data Columns (total " + columnCount + " columns) :");
        showDescribeHeader(indexCellSize, columnCellSize);
        showDescribeValues(columnCount, indexCellSize, columnCellSize);
        showDtypes();
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

    private void showDtypes() {
        String builder = "dtypes: int(" + numericColumnCount + "), String(" + (columns.size() - numericColumnCount) + ")";
        System.out.println(builder);
    }

    @Override
    public Table head() {
        int lineCount = Math.min(columns.size(), DEFAULT_ROW_SIZE);
        return selectRows(0, lineCount);
    }

    @Override
    public Table head(int lineCount) {
        lineCount = Math.min(lineCount, entrySize);
        return selectRows(0, lineCount);
    }

    @Override
    public Table tail() {
        return selectTopDown(entrySize - DEFAULT_ROW_SIZE, entrySize);
    }

    @Override
    public Table tail(int lineCount) {
        int indexOffset = lineCount - 1;
        int lastIndex = entrySize - 1;
        int startIndex = lastIndex - indexOffset;
        if (lineCount > entrySize) {
            startIndex = 0;
        }
        return selectTopDown(startIndex, entrySize);
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        if (beginIndex < endIndex) {
            return selectTopDown(beginIndex, endIndex);
        }
        return selectBottomUp(beginIndex, endIndex);
    }

    private Table selectTopDown(int beginIndex, int endIndex) {
        Table table = selectOneRow(beginIndex);
        for (int i = beginIndex + 1; i < endIndex; i++) {
            table = union(table, selectOneRow(i));
        }
        return table;
    }

    private Table selectBottomUp(int beginIndex, int endIndex) {
        Table table = selectOneRow(beginIndex);
        for (int i = beginIndex - 1; i > endIndex; i--) {
            table = union(table, selectOneRow(i));
        }
        return table;
    }

    private Table selectOneRow(int index) {
        List<Column> copyColumns = columns.stream()
                .map(column -> new ColumnImpl(column.getHeader(), List.of(column.getValue(index))))
                .collect(Collectors.toList());
        return new TableImpl(this.getName(), copyColumns);
    }

    private Table union(Table one, Table another) {
        validateSameColumnSize(one, another);
        List<Column> newColumns = new ArrayList<>();
        for (int i = 0; i < one.getColumnCount(); i++) {
            Column oneColumn = one.getColumn(i);
            String header = oneColumn.getHeader();
            Column anoterColumn = another.getColumn(header);
            Column concatColumn = concatColumn(oneColumn, anoterColumn);
            newColumns.add(concatColumn);
        }
        return new TableImpl(one.getName(), newColumns);
    }

    public Column concatColumn(Column one, Column another) {
        List<String> copyValues = new ArrayList<>();
        for (int i = 0; i < one.count(); i++) {
            copyValues.add(one.getValue(i));
        }
        for (int i = 0; i < another.count(); i++) {
            copyValues.add(another.getValue(i));
        }
        return new ColumnImpl(one.getHeader(), copyValues);
    }

    public Column concatColumn(String name, Column one, Column another) {
        List<String> copyValues = new ArrayList<>();
        for (int i = 0; i < one.count(); i++) {
            copyValues.add(one.getValue(i));
        }
        for (int i = 0; i < another.count(); i++) {
            copyValues.add(another.getValue(i));
        }
        return new ColumnImpl(name, copyValues);
    }

    private void validateSameColumnSize(Table one, Table another) {
        if (one.getColumnCount() != another.getColumnCount()) {
            throw new IllegalArgumentException("can't union, columns size diff");
        }
    }

    @Override
    public Table selectRowsAt(int... indices) {
        Table newTable = selectOneRow(indices[0]);
        for (int i = 1; i < indices.length; i++) {
            newTable = union(newTable, selectOneRow(indices[i]));
        }
        return newTable;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        List<Column> newColumns = columns.stream()
                .skip(beginIndex)
                .limit(endIndex - beginIndex)
                .collect(Collectors.toList());
        return new TableImpl(this.tableName, newColumns);
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        List<Column> newColumns = Arrays.stream(indices)
                .mapToObj(columns::get)
                .collect(Collectors.toList());
        return new TableImpl(this.tableName, newColumns);
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
/*        Column targetColumn = getColumn(columnName);
        List<Integer> rowIndices = new ArrayList<>();
        for (int i = 0; i < targetColumn.count(); i++) {
            if (predicate.test(targetColumn.getValue(i))) {
                rowIndices.add(i);
            }
        }
        Table newTable = selectOneRow(rowIndices.get(0));
        for (int i = 1; i < rowIndices.size(); i++) {
            newTable = union(newTable, selectOneRow(rowIndices.get(i)));
        }
        Predicate<String> predicate1 = (Predicate<String>) predicate;

        return newTable;*/
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        List<Table> rowList = splitToRows(this);
        List<Table> nullList = getNullRows(byIndexOfColumn, rowList);
        Table nullTable = unionTables(nullList);
        Table sortedTable = sortTableWithoutNull(byIndexOfColumn, isAscending, rowList);
        Table union = unionNulltoTop(isNullFirst, nullTable, sortedTable);
        for (int i = 0; i < columns.size(); i++) {
            this.columns.set(i, union.getColumn(i));
        }
        return this;
    }

    private Table unionNulltoTop(boolean isNullFirst, Table nullTable, Table sortedTable) {
        if (isNullFirst) {
            return union(nullTable, sortedTable);
        }
        return union(sortedTable, nullTable);
    }

    private Table sortTableWithoutNull(int byIndexOfColumn, boolean isAscending, List<Table> rowList) {
        if (isAscending) {
            return ascendingSort(byIndexOfColumn, rowList);
        }
        return reversedSort(byIndexOfColumn, rowList);
    }

    private List<Table> splitToRows(Table table) {
        List<Table> rowList = new ArrayList<>();
        for (int i = 0; i < entrySize; i++) {
            rowList.add(table.selectRowsAt(i));
        }
        return rowList;
    }

    private static List<Table> getNullRows(int byIndexOfColumn, List<Table> rowList) {
        return rowList.stream()
                .filter(table -> table.getColumn(byIndexOfColumn).getValue(0).equals(""))
                .collect(Collectors.toList());
    }

    private Table ascendingSort(int byIndexOfColumn, List<Table> rowList) {
        List<Table> sortedRows = rowList.stream()
                .filter(table -> !table.getColumn(byIndexOfColumn).getValue(0).equals(""))
                .sorted(Comparator.comparing(a -> a.getColumn(byIndexOfColumn).getValue(0)))
                .collect(Collectors.toList());
        sortedRows.get(0).getColumn(byIndexOfColumn).getValue(0);
        return unionTables(sortedRows);
    }

    private Table reversedSort(int byIndexOfColumn, List<Table> rowList) {
        List<Table> sortedRows = rowList.stream()
                .filter(table -> !table.getColumn(byIndexOfColumn).getValue(0).equals(""))
                .sorted(Comparator.comparing(a -> a.getColumn(byIndexOfColumn).getValue(0), Comparator.reverseOrder()))
                .collect(Collectors.toList());
        sortedRows.get(0).getColumn(byIndexOfColumn).getValue(0);
        return unionTables(sortedRows);
    }

    private Table unionTables(List<Table> tables) {
        Table unionTables = tables.get(0);
        for (int i = 1; i < tables.size(); i++) {
            unionTables = union(unionTables, tables.get(i));
        }
        return unionTables;
    }

    @Override
    public int getRowCount() {
        return entrySize;
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public Column getColumn(int index) {
        return this.columns.get(index);
    }

    @Override
    public Column getColumn(String name) {
        return columns.stream()
                .filter(column -> column.getHeader().equals(name))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("can't find column"));
    }

    @Override
    public String toString() {
        return "<database.table@" + Integer.toHexString(hashCode()) + ">";
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        TableImpl table = (TableImpl) o;
//        return Objects.equals(columns, table.columns);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(columns, tableName);
//    }
}
