package org.tarantool.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class JdbcConstants {

    public static void checkHoldabilityConstant(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT &&
            holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLNonTransientException("", SQLStates.INVALID_PARAMETER_VALUE.getSqlState());
        }
    }

}
