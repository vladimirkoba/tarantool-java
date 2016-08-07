package org.tarantool.jdbc.adapter;

import java.sql.SQLException;

public class EmptyResponseAdapter implements ResponseAdapter {
    public static final EmptyResponseAdapter INSTANCE = new EmptyResponseAdapter();

    @Override
    public boolean isNull(int columnIndex) throws SQLException {
        throw new SQLException("Result set is empty. You should not ignore next return value :)");
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        throw new SQLException("Result set is empty. You should not ignore next return value :)");
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        throw new SQLException("Result set is empty. You should not ignore next return value :)");
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public Number getNumber(int columnIndex) throws SQLException {
        throw new SQLException("Result set is empty. You should not ignore next return value :)");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLException("Result set is empty. You should not ignore next return value :)");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throw new SQLException("Result set is empty. You should not ignore next return value :)");
    }

    @Override
    public int getRow() {
        return 0;
    }

    @Override
    public String toString() {
        return "EmptyResponseAdapter{}";
    }
}
