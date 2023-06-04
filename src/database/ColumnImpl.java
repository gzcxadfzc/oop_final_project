package database;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class ColumnImpl implements Column {

    private static final String NULL_TEXT = "null";
    private static final String START_LINE = "| ";
    private static final String END_LINE = " |";
    private static final String SPACE = " ";
    private static final String INTEGER_REGEX = "[+-]?\\d+";
    private final String header;
    private final boolean isNumeric;
    private int cellSize;
    private List<String> data;

    public ColumnImpl(String header, List<String> data) {
        this.header = header;
        this.data = data;
        this.isNumeric = isAllInteger(data);
        this.cellSize = findLongestLength();
    }

    private int findLongestLength() {
        int maxDataLength = data.stream()
                .map(String::length)
                .mapToInt(Integer::intValue)
                .max()
                .orElse(0);
        return Math.max(maxDataLength, header.length());
    }

    private boolean isAllInteger(List<String> data) {
        int numericCount = (int) data.stream()
                .filter(value -> value.matches(INTEGER_REGEX))
                .count();
        return numericCount == data.size();
    }

    @Override
    public String getHeader() {
        return header;
    }

    @Override
    public String getValue(int index) {
        return data.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        try {
            double value = Double.parseDouble(data.get(index));
            if (t.equals(Integer.class)) {
                return t.cast((int) value);
            }
            if (t.equals(Float.class)) {
                return t.cast((float) value);
            }
            return t.cast(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not a numeric type");
        }
    }

    @Override
    public void setValue(int index, String value) {
        List<String> mutableCopyData = new ArrayList<>(data);
        mutableCopyData.set(index, value);
        data = mutableCopyData;
        cellSize = Math.max(value.length(), cellSize);
    }

    @Override
    public void setValue(int index, int value) {
        String numericValue = Integer.toString(value);
        setValue(index, numericValue);
        cellSize = Math.max(numericValue.length(), cellSize);
    }

    @Override
    public int count() {
        return data.size();
    }

    @Override
    public void show() {
        showOneCell(header, cellSize);
        for (String value : data) {
            showOneCell(value, cellSize);
        }
    }

    private void showOneCell(String value, int cellSize) {
        StringBuilder cellText = new StringBuilder(START_LINE);
        String printValue = convertNull(value);
        cellText.append(SPACE.repeat(Math.max(0, cellSize - printValue.length())));
        cellText.append(printValue);
        cellText.append(END_LINE);
        System.out.println(cellText);
    }

    private static String convertNull(String value) {
        if (value.equals("")) {
            return NULL_TEXT;
        }
        return value;
    }

    @Override
    public boolean isNumericColumn() {
        return isNumeric;
    }

    @Override
    public long getNullCount() {
        return data.stream()
                .filter(value -> value.matches(""))
                .count();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnImpl column = (ColumnImpl) o;
        return Objects.equals(header, column.header) && Objects.equals(data, column.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, data);
    }
}
