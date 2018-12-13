package org.tarantool.jdbc;

/**
 * Enumeration of SQL types recognizable by tarantool.
 */
public enum TntSqlType {
    FLOAT("FLOAT"),

    DOUBLE("DOUBLE"),

    REAL("REAL"),

    INT("INT"),
    INTEGER("INTEGER"),

    DECIMAL("DECIMAL"),
    DECIMAL_PREC("DECIMAL(20)"),
    DECIMAL_PREC_SCALE("DECIMAL(20,10)"),

    NUMERIC("NUMERIC"),
    NUMERIC_PREC("NUMERIC(20)"),
    NUMERIC_PREC_SCALE("NUMERIC(20,10)"),

    NUM("NUM"),
    NUM_PREC("NUM(20)"),
    NUM_PREC_SCALE("NUM(20,10)"),

    CHAR("CHAR(128)"),

    VARCHAR("VARCHAR(128)"),

    TEXT("TEXT"),

    BLOB("BLOB");

    //DATE("DATE"),
    //TIME("TIME"),
    //TIMESTAMP("TIMESTAMP"),

    public String sqlType;

    TntSqlType(String sqlType) {
        this.sqlType = sqlType;
    }
}
