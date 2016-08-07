package org.tarantool.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.tarantool.jdbc.adapter.ResponseAdapter;
import org.tarantool.jdbc.mock.AbstractResultSet;

public class ResultSet extends AbstractResultSet {
    private final ResponseAdapter adapter;

    public ResultSet(ResponseAdapter adapter) {
        this.adapter = adapter;
    }

    @Override
    public boolean next() throws SQLException {
        return adapter.next();
    }

    @Override
    public void close() throws SQLException {
        adapter.close();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return adapter.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return adapter.getObject(findColumn(columnLabel));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return adapter.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        if(object == null) {
            return false;
        }
        if (object instanceof Boolean) {
            return (Boolean) object;
        }
        if (object instanceof Number) {
            return ((Number) object).intValue() > 0;
        }
        return Boolean.parseBoolean(String.valueOf(object));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return adapter.getNumber(columnIndex).byteValue();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return adapter.getNumber(columnIndex).shortValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return adapter.getNumber(columnIndex).intValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return adapter.getNumber(columnIndex).longValue();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return adapter.getNumber(columnIndex).floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return adapter.getNumber(columnIndex).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        if (bd != null) {
            bd.setScale(scale);
        }
        return bd;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return adapter.getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return adapter.isNull(columnIndex) ? null : new Date(getLong(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return adapter.isNull(columnIndex) ? null : new Time(getLong(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return adapter.isNull(columnIndex) ? null : new Timestamp(getLong(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return this.getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return this.getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return this.getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return this.getBigDecimal(columnLabel, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return this.getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return this.getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.getTimestamp(findColumn(columnLabel));
    }


    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return adapter.findColumn(columnLabel);
    }

    @Override
    public int getRow() throws SQLException {
        return adapter.getRow();
    }

    @Override
    public String toString() {
        return adapter.toString();
    }
}
