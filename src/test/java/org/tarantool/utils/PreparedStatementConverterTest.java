package org.tarantool.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.tarantool.jdbc.SQLQueryHolder;

class PreparedStatementConverterTest {


  @Test
  public void test() {
    String sql = "INSERT INTO sub_agreements (\n" +
        "    sub_agreement_id, bucket_id, agreement_id, name, diasoft_code, client_code, is_main,\n" +
        "    type_flags, partner, status, open_date, close_date, tariff_id,\n" +
        "    tariff_name, last_change_time, mdm_id, client_info_id\n" +
        ")\n" +
        "VALUES (\n" +
        "           'c0d74ee7-39ac-4468-91de-03c38bb2ab44',\n" +
        "           1,\n" +
        "           '573d4e51-1aac-45d7-aa9e-d64e20ea6421',\n" +
        "           null,\n" +
        "           '454534',\n" +
        "           '454534',\n" +
        "           true,\n" +
        "           '{1231}',\n" +
        "           null,\n" +
        "           'ACTIVE',\n" +
        "           null,\n" +
        "           null,\n" +
        "           null,\n" +
        "           null,\n" +
        "           null,\n" +
        "           '454534',\n" +
        "           'f685d293-54a4-47d0-95cc-453a4ec521bc'\n" +
        "       )";

    SQLQueryHolder queryHolder = PreparedStatementConverter.convertSqlToPreparedStatementFormat(sql);

    System.out.println("Parameterized Query:");
    System.out.println(queryHolder.getQuery());
    System.out.println(queryHolder.getParams());
  }
}