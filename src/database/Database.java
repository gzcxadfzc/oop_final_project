package database;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Database {
    // 테이블명이 같으면 같은 테이블로 간주된다.
    private static final Set<Table> tables = new HashSet<>();

    // 테이블 이름 목록을 출력한다.
    public static void showTables() {
        tables.forEach(table -> System.out.println(table.getName()));
    }

    /**
     * 파일로부터 테이블을 생성하고 table에 추가한다.
     *
     * @param csv 확장자는 csv로 가정한다.
     *            파일명이 테이블명이 된다.
     *            csv 파일의 1행은 컬럼명으로 사용한다.
     *            csv 파일의 컬럼명은 중복되지 않는다고 가정한다.
     *            컬럼의 데이터 타입은 int 아니면 String으로 판정한다.
     *            String 타입의 데이터는 ("), ('), (,)는 포함하지 않는 것으로 가정한다.
     */
    public static void createTable(File csv) throws FileNotFoundException {
        FileReader reader = new FileReader(csv);
        try {
            BufferedReader bufferedReader = new BufferedReader(reader);
            String delimiter = ",";
            String line;
            List<List<String>> data = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                String[] values = line.split(delimiter, -1);
                if (data.isEmpty()) {
                    for (int i = 0; i < values.length; i++) {
                        data.add(new ArrayList<>());
                    }
                }
                for (int i = 0; i < values.length; i++) {
                    data.get(i).add(values[i]);
                }
            }
            List<Column> columns = data.stream()
                    .map(value -> new ColumnImpl(value.get(0), value.subList(1, value.size())))
                    .collect(Collectors.toList());
            String tableName = csv.getName()
                    .substring(0, csv.getName().length() - 4);
            tables.add(new TableImpl(tableName, columns));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {

        }
    }

    // tableName과 테이블명이 같은 테이블을 리턴한다. 없으면 null 리턴.
    public static Table getTable(String tableName) {
        return tables.stream()
                .filter(table -> table.getName().equals(tableName))
                .findAny()
                .orElse(null);
    }

    /**
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생시켜도 됨.
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        String copyName = table.getName();
        List<Column> copyColumns = new ArrayList<>();
        for (int i = 0; i < table.getColumnCount(); i++) {
            copyColumns.add(table.getColumn(i));
        }
        return new TableImpl(copyName, copyColumns).sort(byIndexOfColumn, isAscending, isNullFirst);
    }
}
