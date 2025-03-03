package org.tarantool.handle.wrapper;

import static org.tarantool.handle.wrapper.RequestTypeWrapper.processDelete;
import static org.tarantool.handle.wrapper.RequestTypeWrapper.processInsert;
import static org.tarantool.handle.wrapper.RequestTypeWrapper.processSelect;
import static org.tarantool.handle.wrapper.RequestTypeWrapper.processUpdate;
import static org.tarantool.logging.LocalLogger.log;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class RequestWrapper {

  /**
   * Method to add double quotes around table and column names, remove aliases from UPDATE, DELETE, and INSERT statements in SELECT, UPDATE, and
   * DELETE statements.
   *
   * @param sql The original SQL query.
   * @return The modified SQL query.
   */
  public static String wrap(String sql) {
    try {
      log("sql = {0}", sql);
      if (sql == null || sql.trim().isEmpty()) {
        return sql;
      }

      // Parse the SQL statement
      Statement statement = CCJSqlParserUtil.parse(sql);

      // Process based on the type of statement
      if (statement instanceof Select) {
        processSelect((Select) statement);
      } else if (statement instanceof Insert) {
        processInsert((Insert) statement);
      } else if (statement instanceof Update) {
        processUpdate((Update) statement);
      } else if (statement instanceof Delete) {
        processDelete((Delete) statement);
      }

      // Return the modified SQL statement
      return statement.toString();
    } catch (JSQLParserException e) {
      throw new RuntimeException("Failed to parse SQL: " + sql, e);
    }
  }
}