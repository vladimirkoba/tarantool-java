package org.tarantool.utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;

public class QuoteWrapper {

  /**
   * Method to add double quotes around table and column names, remove aliases from UPDATE, DELETE, and INSERT statements,
   * and replace LIKE ? ESCAPE '#' with = ? in SELECT, UPDATE, and DELETE statements.
   *
   * @param sql The original SQL query.
   * @return The modified SQL query.
   */
  public static String addQuotesForSpaces(String sql) {
    try {
      LocalLogger.log("sql = " + sql);
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

  /**
   * Processes SELECT statements to wrap table and column names in double quotes,
   * and replace LIKE ? ESCAPE '#' with = ?.
   *
   * @param select The SELECT statement.
   */
  private static void processSelect(Select select) {
    SelectBody selectBody = select.getSelectBody();

    selectBody.accept(new SelectVisitorAdapter() {
      @Override
      public void visit(PlainSelect plainSelect) {
        // Process FROM item
        wrapFromItem(plainSelect.getFromItem());

        // Process JOINs
        if (plainSelect.getJoins() != null) {
          for (Join join : plainSelect.getJoins()) {
            wrapFromItem(join.getRightItem());
            if (join.getOnExpression() != null) {
              Expression newOn = processExpression(join.getOnExpression());
              join.setOnExpression(newOn);
            }
          }
        }

        // Process SELECT items
        if (plainSelect.getSelectItems() != null) {
          for (SelectItem item : plainSelect.getSelectItems()) {
            item.accept(new SelectItemVisitorAdapter() {
              @Override
              public void visit(SelectExpressionItem selectExpressionItem) {
                Expression expr = processExpression(selectExpressionItem.getExpression());
                selectExpressionItem.setExpression(expr);
              }
            });
          }
        }

        // Process WHERE clause
        if (plainSelect.getWhere() != null) {
          Expression newWhere = processExpression(plainSelect.getWhere());
          plainSelect.setWhere(newWhere);
        }

        // Process ORDER BY elements
        if (plainSelect.getOrderByElements() != null) {
          for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
            Expression expr = processExpression(orderByElement.getExpression());
            orderByElement.setExpression(expr);
          }
        }
      }
    });
  }

  /**
   * Processes INSERT statements to wrap table and column names in double quotes and remove aliases.
   *
   * @param insert The INSERT statement.
   */
  private static void processInsert(Insert insert) {
    // Wrap table name and remove alias
    wrapTableName(insert.getTable(), true);

    // Wrap column names
    if (insert.getColumns() != null) {
      for (Column column : insert.getColumns()) {
        wrapColumnName(column);
      }
    }

    // Process SELECT part if present (INSERT INTO ... SELECT ...)
    if (insert.getSelect() != null) {
      processSelect(insert.getSelect());
    }
  }

  /**
   * Processes UPDATE statements to wrap table and column names in double quotes, remove aliases,
   * and replace LIKE ? ESCAPE '#' with = ? in WHERE clause.
   *
   * @param update The UPDATE statement.
   */
  private static void processUpdate(Update update) {
    // Wrap table name and remove alias
    wrapTableName(update.getTable(), true);

    // Wrap column names in SET expressions
    if (update.getColumns() != null) {
      for (Column column : update.getColumns()) {
        wrapColumnName(column);
      }
    }

    // Wrap expressions in SET expressions
    if (update.getExpressions() != null) {
      for (int i = 0; i < update.getExpressions().size(); i++) {
        Expression expr = update.getExpressions().get(i);
        Expression newExpr = processExpression(expr);
        update.getExpressions().set(i, newExpr);
      }
    }

    // Process WHERE clause
    if (update.getWhere() != null) {
      Expression newWhere = processExpression(update.getWhere());
      update.setWhere(newWhere);
    }
  }

  /**
   * Processes DELETE statements to wrap table and column names in double quotes, remove aliases,
   * and replace LIKE ? ESCAPE '#' with = ? in WHERE clause.
   *
   * @param delete The DELETE statement.
   */
  private static void processDelete(Delete delete) {
    // Wrap table name and remove alias
    wrapTableName(delete.getTable(), true);

    // Process WHERE clause
    if (delete.getWhere() != null) {
      Expression newWhere = processExpression(delete.getWhere());
      delete.setWhere(newWhere);
    }
  }

  /**
   * Wraps FROM items in SELECT statements.
   *
   * @param fromItem The FROM item.
   */
  private static void wrapFromItem(FromItem fromItem) {
    if (fromItem instanceof Table) {
      wrapTableName((Table) fromItem, false);
    } else if (fromItem instanceof SubSelect) {
      SubSelect subSelect = (SubSelect) fromItem;
      if (subSelect.getSelectBody() != null) {
        processSelect(new Select() {{
          setSelectBody(subSelect.getSelectBody());
        }});
      }
    }
    // Handle other FromItem types if necessary
  }

  /**
   * Wraps table name in double quotes and optionally removes aliases.
   *
   * @param table       The table.
   * @param removeAlias Whether to remove alias.
   */
  private static void wrapTableName(Table table, boolean removeAlias) {
    if (table == null) {
      return;
    }

    String tableName = stripQuotes(table.getName());
    table.setName("\"" + tableName + "\"");

    if (removeAlias) {
      table.setAlias(null);
    }
  }

  /**
   * Wraps column name in double quotes and removes table references from columns.
   *
   * @param column The column.
   */
  private static void wrapColumnName(Column column) {
    if (column == null) {
      return;
    }

    String columnName = stripQuotes(column.getColumnName());
    column.setColumnName("\"" + columnName + "\"");
    column.setTable(null); // Remove table references
  }

  /**
   * Processes expressions by visiting their components, wrapping column names,
   * and replacing LIKE ? ESCAPE '#' with = ?.
   *
   * @param expression The expression.
   * @return The processed (and possibly modified) expression.
   */
  private static Expression processExpression(Expression expression) {
    if (expression == null) {
      return null;
    }

    if (expression instanceof LikeExpression) {
      LikeExpression likeExpr = (LikeExpression) expression;
      // Process left and right expressions
      Expression leftExpr = processExpression(likeExpr.getLeftExpression());
      Expression rightExpr = processExpression(likeExpr.getRightExpression());
      likeExpr.setLeftExpression(leftExpr);
      likeExpr.setRightExpression(rightExpr);

      // Check if escape character is '#'
      String escape = likeExpr.getEscape().toString();
      if (escape != null) {
        escape = stripQuotes(escape);
      }
      if ("#".equals(escape)) {
        // Create EqualsTo expression
        EqualsTo equalsTo = new EqualsTo();
        equalsTo.setLeftExpression(leftExpr);
        equalsTo.setRightExpression(rightExpr);
        return equalsTo;
      }
      return likeExpr;
    } else if (expression instanceof BinaryExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expression;
      Expression leftExpr = processExpression(binaryExpr.getLeftExpression());
      Expression rightExpr = processExpression(binaryExpr.getRightExpression());
      binaryExpr.setLeftExpression(leftExpr);
      binaryExpr.setRightExpression(rightExpr);
      return binaryExpr;
    } else if (expression instanceof Parenthesis) {
      Parenthesis parenthesis = (Parenthesis) expression;
      Expression innerExpr = processExpression(parenthesis.getExpression());
      parenthesis.setExpression(innerExpr);
      return parenthesis;
    } else if (expression instanceof Column) {
      wrapColumnName((Column) expression);
      return expression;
    } else {
      // Process any nested expressions
      expression.accept(new ExpressionVisitorAdapter() {
        @Override
        public void visit(Column column) {
          wrapColumnName(column);
        }
      });
      return expression;
    }
  }

  /**
   * Removes existing quotes from an identifier.
   *
   * @param name The identifier name.
   * @return The unquoted identifier name.
   */
  private static String stripQuotes(String name) {
    if (name == null || name.isEmpty()) {
      return name;
    }
    if ((name.startsWith("\"") && name.endsWith("\"")) ||
        (name.startsWith("`") && name.endsWith("`")) ||
        (name.startsWith("'") && name.endsWith("'"))) {
      return name.substring(1, name.length() - 1);
    }
    return name;
  }

  /**
   * Main method containing test cases for the QuoteWrapper class.
   *
   * @param args Command-line arguments.
   */
  public static void main(String[] args) {
    // Test Case 1: Simple SELECT
    String sql1 = "SELECT id, name, age FROM users";
    String expected1 = "SELECT \"id\", \"name\", \"age\" FROM \"users\"";
    String result1 = addQuotesForSpaces(sql1);
    assert result1.equals(expected1) : "Test Case 1 Failed";

    // Test Case 2: SELECT with ORDER BY
    String sql2 = "SELECT id, name FROM employees ORDER BY name DESC, id ASC";
    String expected2 = "SELECT \"id\", \"name\" FROM \"employees\" ORDER BY \"name\" DESC, \"id\" ASC";
    String result2 = addQuotesForSpaces(sql2);
    assert result2.equals(expected2) : "Test Case 2 Failed";

    // Test Case 3: SELECT with LIKE ? ESCAPE '#'
    String sql3 = "SELECT * FROM products WHERE description LIKE ? ESCAPE '#'";
    String expected3 = "SELECT * FROM \"products\" WHERE \"description\" = ?";
    String result3 = addQuotesForSpaces(sql3);
    assert result3.equals(expected3) : "Test Case 3 Failed";

    // Test Case 4: INSERT Statement with Aliases
    String sql4 = "INSERT INTO orders (order_id, customer_name) VALUES (?, ?)";
    String expected4 = "INSERT INTO \"orders\" (\"order_id\", \"customer_name\") VALUES (?, ?)";
    String result4 = addQuotesForSpaces(sql4);
    assert result4.equals(expected4) : "Test Case 4 Failed";

    // Test Case 5: UPDATE Statement with Aliases and WHERE clause
    String sql5 = "UPDATE employees e SET e.salary = ? WHERE e.id = ?";
    String expected5 = "UPDATE \"employees\" SET \"salary\" = ? WHERE \"id\" = ?";
    String result5 = addQuotesForSpaces(sql5);
    assert result5.equals(expected5) : "Test Case 5 Failed";

    // Test Case 6: DELETE Statement with WHERE clause
    String sql6 = "DELETE FROM sessions s WHERE s.user_id = ?";
    String expected6 = "DELETE FROM \"sessions\" WHERE \"user_id\" = ?";
    String result6 = addQuotesForSpaces(sql6);
    assert result6.equals(expected6) : "Test Case 6 Failed";

    // Test Case 7: SELECT with JOIN and LIKE ? ESCAPE '#'
    String sql7 = "SELECT u.id, u.name, o.order_date FROM users u JOIN orders o ON u.id = o.user_id WHERE o.description LIKE ? ESCAPE '#'";
    String expected7 = "SELECT \"id\", \"name\", \"order_date\" FROM \"users\" JOIN \"orders\" ON \"id\" = \"user_id\" WHERE \"description\" = ?";
    String result7 = addQuotesForSpaces(sql7);
    assert result7.equals(expected7) : "Test Case 7 Failed";

    // Test Case 8: NULL SQL
    String sql8 = null;
    String expected8 = null;
    String result8 = addQuotesForSpaces(sql8);
    assert result8 == expected8 : "Test Case 8 Failed";

    // Test Case 9: Empty SQL
    String sql9 = "   ";
    String expected9 = "   ";
    String result9 = addQuotesForSpaces(sql9);
    assert result9.equals(expected9) : "Test Case 9 Failed";

    // Test Case 10: Complex WHERE Clause with Multiple Conditions
    String sql10 = "SELECT * FROM inventory WHERE category = ? AND name LIKE ? ESCAPE '#' OR quantity > ?";
    String expected10 = "SELECT * FROM \"inventory\" WHERE \"category\" = ? AND \"name\" = ? OR \"quantity\" > ?";
    String result10 = addQuotesForSpaces(sql10);
    assert result10.equals(expected10) : "Test Case 10 Failed";

    // Test Case 11: UPDATE with LIKE ? ESCAPE '#'
    String sql11 = "UPDATE products SET name = ? WHERE description LIKE ? ESCAPE '#'";
    String expected11 = "UPDATE \"products\" SET \"name\" = ? WHERE \"description\" = ?";
    String result11 = addQuotesForSpaces(sql11);
    assert result11.equals(expected11) : "Test Case 11 Failed";

    // Test Case 12: DELETE with LIKE ? ESCAPE '#'
    String sql12 = "DELETE FROM orders WHERE order_code LIKE ? ESCAPE '#'";
    String expected12 = "DELETE FROM \"orders\" WHERE \"order_code\" = ?";
    String result12 = addQuotesForSpaces(sql12);
    assert result12.equals(expected12) : "Test Case 12 Failed";

    // Test Case 13: UPDATE with multiple LIKE ? ESCAPE '#' conditions
    String sql13 = "UPDATE security_type SET version = ? WHERE id = ? AND bucket_id = ? AND version LIKE ? ESCAPE '#' AND name LIKE ? ESCAPE '#' AND type LIKE ? ESCAPE '#' AND cat_name IS NULL";
    String expected13 = "UPDATE \"security_type\" SET \"version\" = ? WHERE \"id\" = ? AND \"bucket_id\" = ? AND \"version\" = ? AND \"name\" = ? AND \"type\" = ? AND \"cat_name\" IS NULL";
    String result13 = addQuotesForSpaces(sql13);
    assert result13.equals(expected13) : "Test Case 13 Failed";

    System.out.println("All test cases passed successfully!");
  }

}
