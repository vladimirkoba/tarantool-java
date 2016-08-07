package org.tarantool.jdbc.adapter;


import java.sql.SQLException;

public interface ResponseAdapter {

    boolean isNull(int columnIndex) throws SQLException;;

    String getString(int columnIndex) throws SQLException;

    int findColumn(String columnName) throws SQLException;

    boolean next();

    void close() throws SQLException;

    Number getNumber(int columnIndex) throws SQLException;

    byte[] getBytes(int columnIndex) throws SQLException;

    Object getObject(int columnIndex) throws SQLException;

    int getRow();
}
