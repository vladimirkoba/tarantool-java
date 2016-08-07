package org.tarantool.jdbc.adapter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class ResponseAdapterImpl implements ResponseAdapter {
    protected List<List<?>> rows;
    protected Map<String, Integer> columns;
    protected List<?> row;
    protected ListIterator<List<?>> iterator;

    protected ResponseAdapterImpl(List<List<?>> rows, List<String> columns) {
        this.columns = new HashMap<>(columns.size());
        for(int i = 0;i<columns.size();i++) {
            this.columns.put(columns.get(i), i + 1);
        }
        this.rows = rows;
        this.iterator = rows.listIterator();
    }

    public static ResponseAdapter fromReponse(List<List<List<?>>> response) {
        Iterator<List<List<?>>> i = response.iterator();
        List<List<?>> rows = i.next();
        if (rows.size() == 1) {
            return EmptyResponseAdapter.INSTANCE;
        }
        //[[[hello, c, d], [5, 4, 5]]]
        return new ResponseAdapterImpl(rows.subList(1, rows.size()), (List) rows.get(0));
    }

    @Override
    public boolean isNull(int columnIndex) throws SQLException {
        return getObject(columnIndex) == null;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null : String.valueOf(object);
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        Integer i = columns.get(columnName);
        if (i != null) {
            return i;
        }
        throw new SQLException(columnName + " not found");
    }

    @Override
    public boolean next() {
        if (iterator.hasNext()) {
            row = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public Number getNumber(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return new BigDecimal((String) object);
        }
        if (object instanceof Number) {
            return (Number) object;
        }
        throw new SQLException(String.valueOf(object) + " can't be converted to Number");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return (byte[]) getObject(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (row == null) {
            throw new SQLException("Current row is not set. You should call next() first.");
        }
        if (columnIndex < 1 || columnIndex > row.size()) {
            throw new SQLException("Column index should be between " + 1 + " and " + row.size() + " but got " + columnIndex);
        }
        return row.get(columnIndex - 1);
    }

    @Override
    public int getRow() {
        return row == null ? 0 : iterator.nextIndex();
    }

    @Override
    public String toString() {
        return "ResponseAdapterImpl{" +
                "columns=" + columns +
                ", iterator=" + (iterator.nextIndex() - 1) +
                ", row=" + row +
                ", rows=" + rows +
                '}';
    }
}
