package biz.digitalindustry.db.query.sql;

import biz.digitalindustry.db.relational.api.RelationalStore;
import biz.digitalindustry.db.relational.api.Row;
import biz.digitalindustry.db.relational.api.TableDefinition;
import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.query.QueryNode;
import biz.digitalindustry.db.query.QueryCommand;
import biz.digitalindustry.db.query.QueryProvider;
import biz.digitalindustry.db.query.QueryResult;
import biz.digitalindustry.db.query.TextQuerySupport;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.ValueType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlQueryProvider implements QueryProvider {
    private static final Pattern CREATE_TABLE = Pattern.compile(
            "^CREATE\\s+TABLE\\s+([A-Za-z][A-Za-z0-9_]*)\\s*\\((.+)\\)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern CREATE_INDEX = Pattern.compile(
            "^CREATE\\s+INDEX\\s+([A-Za-z][A-Za-z0-9_]*)\\s+ON\\s+([A-Za-z][A-Za-z0-9_]*)\\s*\\(\\s*([A-Za-z][A-Za-z0-9_]*)\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT = Pattern.compile(
            "^INSERT\\s+INTO\\s+([A-Za-z][A-Za-z0-9_]*)\\s*\\((.+)\\)\\s*VALUES\\s*\\((.+)\\)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SELECT_ALL = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+([A-Za-z][A-Za-z0-9_]*)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_WHERE_BETWEEN = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+([A-Za-z][A-Za-z0-9_]*)\\s+WHERE\\s+([A-Za-z][A-Za-z0-9_]*)\\s+BETWEEN\\s+(.+)\\s+AND\\s+(.+)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SELECT_WHERE_EQUALS = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+([A-Za-z][A-Za-z0-9_]*)\\s+WHERE\\s+([A-Za-z][A-Za-z0-9_]*)\\s*=\\s*(.+)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern UPDATE = Pattern.compile(
            "^UPDATE\\s+([A-Za-z][A-Za-z0-9_]*)\\s+SET\\s+(.+)\\s+WHERE\\s+([A-Za-z][A-Za-z0-9_]*)\\s*=\\s*(.+)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern DELETE = Pattern.compile(
            "^DELETE\\s+FROM\\s+([A-Za-z][A-Za-z0-9_]*)\\s+WHERE\\s+([A-Za-z][A-Za-z0-9_]*)\\s*=\\s*(.+)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final RelationalStore relationalStore;

    public SqlQueryProvider(RelationalStore relationalStore) {
        this.relationalStore = relationalStore;
    }

    @Override
    public String queryType() {
        return "sql";
    }

    @Override
    public QueryResult execute(QueryCommand command) {
        String query = TextQuerySupport.requireQueryText(command);
        Matcher matcher = CREATE_TABLE.matcher(query);
        if (matcher.matches()) {
            TableDefinition table = parseCreateTable(matcher.group(1), matcher.group(2));
            relationalStore.registerTable(table);
            return new QueryResult(List.of(Map.of("result", new QueryNode(table.name(), Map.of(
                    "entityType", "table",
                    "created", true,
                    "primaryKeyColumn", table.primaryKeyColumn()
            )))));
        }

        matcher = CREATE_INDEX.matcher(query);
        if (matcher.matches()) {
            TableDefinition updated = appendIndex(matcher.group(2), matcher.group(1), matcher.group(3));
            relationalStore.registerTable(updated);
            return new QueryResult(List.of(Map.of("result", new QueryNode(updated.name(), Map.of(
                    "entityType", "index",
                    "created", true,
                    "indexName", matcher.group(1),
                    "column", matcher.group(3)
            )))));
        }

        matcher = INSERT.matcher(query);
        if (matcher.matches()) {
            TableDefinition table = requireTable(matcher.group(1));
            Row row = parseInsert(table, matcher.group(2), matcher.group(3));
            return rowsResult(List.of(relationalStore.upsert(table, row)));
        }

        matcher = SELECT_WHERE_BETWEEN.matcher(query);
        if (matcher.matches()) {
            TableDefinition table = requireTable(matcher.group(1));
            String column = matcher.group(2);
            FieldValue from = parseValue(table, column, matcher.group(3));
            FieldValue to = parseValue(table, column, matcher.group(4));
            return rowsResult(relationalStore.findRangeBy(table, column, from, to));
        }

        matcher = SELECT_WHERE_EQUALS.matcher(query);
        if (matcher.matches()) {
            TableDefinition table = requireTable(matcher.group(1));
            String column = matcher.group(2);
            FieldValue value = parseValue(table, column, matcher.group(3));
            if (column.equals(table.primaryKeyColumn())) {
                Row row = relationalStore.get(table, primaryKeyString(value));
                return row == null ? new QueryResult(List.of()) : rowsResult(List.of(row));
            }
            return rowsResult(relationalStore.findBy(table, column, value));
        }

        matcher = SELECT_ALL.matcher(query);
        if (matcher.matches()) {
            return rowsResult(relationalStore.getAll(requireTable(matcher.group(1))));
        }

        matcher = UPDATE.matcher(query);
        if (matcher.matches()) {
            TableDefinition table = requireTable(matcher.group(1));
            List<Row> matches = findMatches(table, matcher.group(3), matcher.group(4));
            Map<String, FieldValue> assignments = parseAssignments(table, matcher.group(2));
            List<Row> updated = new ArrayList<>();
            for (Row match : matches) {
                Map<String, FieldValue> values = new LinkedHashMap<>(match.values());
                values.putAll(assignments);
                String primaryKey = primaryKeyString(values.get(table.primaryKeyColumn()));
                updated.add(relationalStore.upsert(table, new Row(primaryKey, values)));
            }
            return rowsResult(updated);
        }

        matcher = DELETE.matcher(query);
        if (matcher.matches()) {
            TableDefinition table = requireTable(matcher.group(1));
            List<Row> matches = findMatches(table, matcher.group(2), matcher.group(3));
            int deleted = 0;
            for (Row match : matches) {
                if (relationalStore.delete(table, match.primaryKey())) {
                    deleted++;
                }
            }
            if (deleted == 0) {
                return new QueryResult(List.of());
            }
            return new QueryResult(List.of(Map.of("result", new QueryNode("deleted", Map.of(
                    "entityType", "result",
                    "deleted", deleted
            )))));
        }

        throw new IllegalArgumentException(
                "Unsupported sql query. Supported forms include CREATE TABLE, CREATE INDEX, INSERT, SELECT, UPDATE, and DELETE");
    }

    private TableDefinition parseCreateTable(String tableName, String columnsBody) {
        List<FieldDefinition> columns = new ArrayList<>();
        String primaryKeyColumn = null;
        for (String rawDefinition : splitCommaSeparated(columnsBody)) {
            String definition = rawDefinition.trim();
            if (definition.isEmpty()) {
                continue;
            }
            List<String> tokens = List.of(definition.split("\\s+"));
            if (tokens.size() < 2) {
                throw new IllegalArgumentException("Invalid column definition: " + definition);
            }
            String columnName = tokens.get(0);
            ValueType type = parseType(tokens.get(1));
            boolean primaryKey = false;
            boolean required = false;
            for (int i = 2; i < tokens.size(); i++) {
                String token = tokens.get(i).toUpperCase(Locale.ROOT);
                if ("PRIMARY".equals(token) && i + 1 < tokens.size() && "KEY".equalsIgnoreCase(tokens.get(i + 1))) {
                    primaryKey = true;
                    required = true;
                    i++;
                } else if ("NOT".equals(token) && i + 1 < tokens.size() && "NULL".equalsIgnoreCase(tokens.get(i + 1))) {
                    required = true;
                    i++;
                }
            }
            if (primaryKey) {
                if (primaryKeyColumn != null) {
                    throw new IllegalArgumentException("Only one PRIMARY KEY column is supported for table '" + tableName + "'");
                }
                primaryKeyColumn = columnName;
            }
            columns.add(new FieldDefinition(columnName, type, required, false));
        }
        if (primaryKeyColumn == null) {
            throw new IllegalArgumentException("CREATE TABLE requires exactly one PRIMARY KEY column");
        }
        return new TableDefinition(tableName, primaryKeyColumn, columns, List.of());
    }

    private TableDefinition appendIndex(String tableName, String indexName, String columnName) {
        TableDefinition table = requireTable(tableName);
        columnDefinition(table, columnName);
        for (IndexDefinition index : table.indexes()) {
            if (index.name().equals(indexName)) {
                throw new IllegalArgumentException("Index '" + indexName + "' already exists on table '" + tableName + "'");
            }
        }
        List<IndexDefinition> updated = new ArrayList<>(table.indexes());
        updated.add(new IndexDefinition(indexName, IndexKind.NON_UNIQUE, List.of(columnName)));
        return new TableDefinition(table.name(), table.primaryKeyColumn(), table.columns(), updated);
    }

    private Row parseInsert(TableDefinition table, String columnsBody, String valuesBody) {
        List<String> columnNames = splitCommaSeparated(columnsBody);
        List<String> rawValues = splitCommaSeparated(valuesBody);
        if (columnNames.size() != rawValues.size()) {
            throw new IllegalArgumentException("INSERT column count does not match values count for table '" + table.name() + "'");
        }
        Map<String, FieldValue> values = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i).trim();
            values.put(columnName, parseValue(table, columnName, rawValues.get(i)));
        }
        FieldValue primaryKeyValue = values.get(table.primaryKeyColumn());
        if (primaryKeyValue == null) {
            throw new IllegalArgumentException("INSERT must include primary key column '" + table.primaryKeyColumn() + "'");
        }
        return new Row(primaryKeyString(primaryKeyValue), values);
    }

    private List<Row> findMatches(TableDefinition table, String columnName, String rawValue) {
        FieldValue value = parseValue(table, columnName, rawValue);
        if (columnName.equals(table.primaryKeyColumn())) {
            Row row = relationalStore.get(table, primaryKeyString(value));
            return row == null ? List.of() : List.of(row);
        }
        return relationalStore.findBy(table, columnName, value);
    }

    private Map<String, FieldValue> parseAssignments(TableDefinition table, String assignmentsBody) {
        Map<String, FieldValue> assignments = new LinkedHashMap<>();
        for (String rawAssignment : splitCommaSeparated(assignmentsBody)) {
            int separator = rawAssignment.indexOf('=');
            if (separator < 1) {
                throw new IllegalArgumentException("Invalid assignment in UPDATE: " + rawAssignment);
            }
            String columnName = rawAssignment.substring(0, separator).trim();
            String rawValue = rawAssignment.substring(separator + 1).trim();
            assignments.put(columnName, parseValue(table, columnName, rawValue));
        }
        return assignments;
    }

    private QueryResult rowsResult(List<Row> rows) {
        List<Map<String, QueryNode>> results = new ArrayList<>();
        for (Row row : rows) {
            if (row != null) {
                results.add(Map.of("row", toQueryNode(row)));
            }
        }
        return new QueryResult(results);
    }

    private QueryNode toQueryNode(Row row) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("entityType", "row");
        for (Map.Entry<String, FieldValue> entry : row.values().entrySet()) {
            properties.put(entry.getKey(), toObject(entry.getValue()));
        }
        return new QueryNode(row.primaryKey(), properties);
    }

    private Object toObject(FieldValue value) {
        if (value instanceof StringValue stringValue) {
            return stringValue.value();
        }
        if (value instanceof LongValue longValue) {
            return longValue.value();
        }
        if (value instanceof DoubleValue doubleValue) {
            return doubleValue.value();
        }
        if (value instanceof BooleanValue booleanValue) {
            return booleanValue.value();
        }
        return value == null ? null : value.toString();
    }

    private TableDefinition requireTable(String tableName) {
        TableDefinition table = relationalStore.table(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Unknown table '" + tableName + "'");
        }
        return table;
    }

    private FieldDefinition columnDefinition(TableDefinition table, String columnName) {
        for (FieldDefinition column : table.columns()) {
            if (column.name().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException("Unknown column '" + columnName + "' for table '" + table.name() + "'");
    }

    private FieldValue parseValue(TableDefinition table, String columnName, String rawValue) {
        ValueType type = columnDefinition(table, columnName).type();
        return parseTypedValue(type, rawValue);
    }

    private FieldValue parseTypedValue(ValueType type, String rawValue) {
        String value = rawValue.trim();
        if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\""))) {
            value = value.substring(1, value.length() - 1);
        }
        return switch (type) {
            case STRING, REFERENCE, ANY -> new StringValue(value);
            case LONG -> new LongValue(Long.parseLong(value));
            case DOUBLE -> new DoubleValue(Double.parseDouble(value));
            case BOOLEAN -> new BooleanValue(Boolean.parseBoolean(value));
            default -> throw new IllegalArgumentException("Unsupported SQL column type: " + type);
        };
    }

    private ValueType parseType(String sqlType) {
        return switch (sqlType.toUpperCase(Locale.ROOT)) {
            case "STRING", "TEXT", "VARCHAR" -> ValueType.STRING;
            case "LONG", "BIGINT", "INT", "INTEGER" -> ValueType.LONG;
            case "DOUBLE", "FLOAT", "REAL" -> ValueType.DOUBLE;
            case "BOOLEAN", "BOOL" -> ValueType.BOOLEAN;
            default -> throw new IllegalArgumentException("Unsupported SQL column type: " + sqlType);
        };
    }

    private List<String> splitCommaSeparated(String input) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (ch == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == ',' && !inSingleQuote && !inDoubleQuote) {
                parts.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        if (current.length() > 0) {
            parts.add(current.toString().trim());
        }
        return parts;
    }

    private String primaryKeyString(FieldValue value) {
        if (value instanceof StringValue stringValue) {
            return stringValue.value();
        }
        if (value instanceof LongValue longValue) {
            return Long.toString(longValue.value());
        }
        if (value instanceof DoubleValue doubleValue) {
            return Double.toString(doubleValue.value());
        }
        if (value instanceof BooleanValue booleanValue) {
            return Boolean.toString(booleanValue.value());
        }
        throw new IllegalArgumentException("Unsupported primary key value: " + value);
    }
}
