package org.tarantool.jdbc;


import java.sql.SQLException;

import org.tarantool.jdbc.adapter.ResponseAdapter;
import org.tarantool.jdbc.mock.AbstractStatement;

public class Statement extends AbstractStatement {
    protected Connection connection;
    protected ResultSet resultSet;
    protected int updatedCount;

    public Statement(Connection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        resultSet = new ResultSet(connection.execute(sql));
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ResponseAdapter adapter = connection.execute(sql);
        return updatedCount;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updatedCount;
    }
}
