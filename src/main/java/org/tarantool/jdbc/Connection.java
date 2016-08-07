package org.tarantool.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.tarantool.TarantoolClient;
import org.tarantool.jdbc.adapter.ResponseAdapter;
import org.tarantool.jdbc.adapter.ResponseAdapterImpl;
import org.tarantool.jdbc.mock.AbstractConnection;

public class Connection extends AbstractConnection {
    private Properties clientInfo;
    private TarantoolClient client;

    public Connection(TarantoolClient client, Properties clientInfo) {
        this.clientInfo = clientInfo;
        this.client = client;
    }

    public ResponseAdapter execute(String sql) throws SQLException {
        try {
            List response = client.syncOps().eval("return require('sql').connect(''):execute(...)", sql);
            return ResponseAdapterImpl.fromReponse(response);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        return new Statement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }


    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }


    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.clientInfo = properties;
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }
}
