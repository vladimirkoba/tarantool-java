package org.tarantool.handle.wrapper;

import static org.tarantool.util.StringUtils.stripQuotes;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class NameWrapper {

  /**
   * Wraps column name in double quotes.
   *
   * @param name The column name.
   */
  public static void wrapColumnName(Column name) {
    if (name == null) {
      return;
    }

    String columnName = stripQuotes(name.getColumnName());
    name.setColumnName("\"" + columnName + "\"");
  }

  /**
   * Wraps table name in double quotes and optionally removes aliases.
   *
   * @param table The table name.
   */
  public static void wrapTableName(Table table) {
    if (table == null) {
      return;
    }

    String tableName = stripQuotes(table.getName());
    table.setName("\"" + tableName + "\"");
  }
}