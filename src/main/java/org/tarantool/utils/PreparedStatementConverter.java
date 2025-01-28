// file: src/main/java/org/tarantool/utils/PreparedStatementConverter.java
package org.tarantool.utils;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.tarantool.jdbc.SQLQueryHolder;

public class PreparedStatementConverter {

  /**
   * Converts a SQL query into a parameterized format suitable for prepared statements.
   * If the query already contains '?', returns it unchanged.
   * Otherwise, it replaces literal values with '?' and collects them into params.
   */
  public static SQLQueryHolder convertSqlToPreparedStatementFormat(String sql) {
    List<Object> params = new ArrayList<>();
    LocalLogger.log("[PreparedStatementConverter] Original SQL: " + sql);
    try {
      Statement statement = CCJSqlParserUtil.parse(sql);
      LocalLogger.log("[PreparedStatementConverter] Parsed Statement: " + statement.getClass().getSimpleName());

      if (statement instanceof Insert) {
        LocalLogger.log("[PreparedStatementConverter] Processing INSERT statement.");
        Insert insert = (Insert) statement;
        processInsertStatement(insert, params);
        String modifiedSql = insert.toString();
        LocalLogger.log("[PreparedStatementConverter] Modified SQL: " + modifiedSql);
        LocalLogger.log("[PreparedStatementConverter] Parameters: " + params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Update) {
        LocalLogger.log("[PreparedStatementConverter] Processing UPDATE statement.");
        Update update = (Update) statement;
        processUpdateStatement(update, params);
        String modifiedSql = update.toString();
        LocalLogger.log("[PreparedStatementConverter] Modified SQL: " + modifiedSql);
        LocalLogger.log("[PreparedStatementConverter] Parameters: " + params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Delete) {
        LocalLogger.log("[PreparedStatementConverter] Processing DELETE statement.");
        Delete delete = (Delete) statement;
        processDeleteStatement(delete, params);
        String modifiedSql = delete.toString();
        LocalLogger.log("[PreparedStatementConverter] Modified SQL: " + modifiedSql);
        LocalLogger.log("[PreparedStatementConverter] Parameters: " + params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Select) {
        LocalLogger.log("[PreparedStatementConverter] Processing SELECT statement.");
        Select select = (Select) statement;
        processSelectStatement(select, params);
        String modifiedSql = select.toString();
        LocalLogger.log("[PreparedStatementConverter] Modified SQL: " + modifiedSql);
        LocalLogger.log("[PreparedStatementConverter] Parameters: " + params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else {
        throw new IllegalArgumentException(
            "Unsupported SQL command. Only INSERT, UPDATE, DELETE, SELECT are supported."
        );
      }
    } catch (Exception e) {
      LocalLogger.log("[PreparedStatementConverter] Error parsing SQL: " + e.getMessage());
      e.printStackTrace(); // Рассмотрите возможность использования метода логгера для стека ошибок
      return SQLQueryHolder.of(sql);
    }
  }

  /* --------------------------------------------------------------------------
     INSERT
  -------------------------------------------------------------------------- */
  private static void processInsertStatement(Insert insert, List<Object> params) {
    LocalLogger.log("[PreparedStatementConverter] Entering processInsertStatement.");
    if (insert.getItemsList() instanceof net.sf.jsqlparser.expression.operators.relational.ExpressionList) {
      net.sf.jsqlparser.expression.operators.relational.ExpressionList exprList =
          (net.sf.jsqlparser.expression.operators.relational.ExpressionList) insert.getItemsList();
      List<Expression> expressions = exprList.getExpressions();
      LocalLogger.log("[PreparedStatementConverter] INSERT expressions count: " + expressions.size());

      for (int i = 0; i < expressions.size(); i++) {
        Expression expr = expressions.get(i);
        if (isLiteralValue(expr)) {
          Object value = extractValue(expr);
          params.add(value);
          expressions.set(i, new JdbcParameter());
        }
      }
    } else {
      throw new IllegalArgumentException("Unsupported INSERT items list type.");
    }
    LocalLogger.log("[PreparedStatementConverter] Exiting processInsertStatement.");
  }

  /* --------------------------------------------------------------------------
     UPDATE
  -------------------------------------------------------------------------- */
  private static void processUpdateStatement(Update update, List<Object> params) {
    LocalLogger.log("[PreparedStatementConverter] Entering processUpdateStatement.");
    List<Expression> expressions = update.getExpressions();
    LocalLogger.log("[PreparedStatementConverter] UPDATE expressions count: " + expressions.size());

    for (int i = 0; i < expressions.size(); i++) {
      Expression expr = expressions.get(i);
      if (isLiteralValue(expr)) {
        Object value = extractValue(expr);
        params.add(value);
        expressions.set(i, new JdbcParameter());
      }
    }

    Expression where = update.getWhere();
    if (where != null) {
      update.setWhere(processExpression(where, params));
    }
    LocalLogger.log("[PreparedStatementConverter] Exiting processUpdateStatement.");
  }

  /* --------------------------------------------------------------------------
     DELETE
  -------------------------------------------------------------------------- */
  private static void processDeleteStatement(Delete delete, List<Object> params) {
    LocalLogger.log("[PreparedStatementConverter] Entering processDeleteStatement.");
    Expression where = delete.getWhere();
    if (where != null) {
      delete.setWhere(processExpression(where, params));
    }
    LocalLogger.log("[PreparedStatementConverter] Exiting processDeleteStatement.");
  }

  /* --------------------------------------------------------------------------
     SELECT
  -------------------------------------------------------------------------- */
  private static void processSelectStatement(Select select, List<Object> params) {
    LocalLogger.log("[PreparedStatementConverter] Entering processSelectStatement.");
    if (select.getSelectBody() instanceof PlainSelect) {
      PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
      Expression where = plainSelect.getWhere();
      if (where != null) {
        plainSelect.setWhere(processExpression(where, params));
      }
    }
    LocalLogger.log("[PreparedStatementConverter] Exiting processSelectStatement.");
  }

  /* --------------------------------------------------------------------------
     Common expression processor
  -------------------------------------------------------------------------- */
  private static Expression processExpression(Expression expr, List<Object> params) {
    if (expr instanceof BinaryExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expr;
      binaryExpr.setLeftExpression(processExpression(binaryExpr.getLeftExpression(), params));
      binaryExpr.setRightExpression(processExpression(binaryExpr.getRightExpression(), params));
      return binaryExpr;
    } else if (expr instanceof Parenthesis) {
      Parenthesis parenExpr = (Parenthesis) expr;
      parenExpr.setExpression(processExpression(parenExpr.getExpression(), params));
      return parenExpr;
    } else if (isLiteralValue(expr)) {
      Object value = extractValue(expr);
      params.add(value);
      return new JdbcParameter();
    } else {
      return expr;
    }
  }

  /**
   * Determines if the expression should be treated as a literal value.
   */
  private static boolean isLiteralValue(Expression expr) {
    boolean isLiteral = expr instanceof LongValue ||
        expr instanceof StringValue ||
        expr instanceof NullValue ||
        expr instanceof DoubleValue ||
        expr instanceof TimestampValue ||
        expr instanceof DateValue ||
        expr instanceof TimeValue ||
        // Обрабатываем булевые литералы, представленные как Column("true") или Column("false")
        isBooleanLiteral(expr);
    return isLiteral;
  }

  /**
   * Checks if the expression is a boolean literal (true/false)
   * при виде "Column('false')" вместо нативного BooleanValue(false).
   */
  private static boolean isBooleanLiteral(Expression expr) {
    if (expr instanceof Column) {
      String columnName = ((Column) expr).getColumnName();
      // Удаляем кавычки, если они присутствуют
      if (columnName.startsWith("\"") && columnName.endsWith("\"") && columnName.length() > 2) {
        columnName = columnName.substring(1, columnName.length() - 1);
      }
      return "true".equalsIgnoreCase(columnName) || "false".equalsIgnoreCase(columnName);
    }
    return false;
  }

  /**
   * Extracts the Java value from supported expression types.
   */
  private static Object extractValue(Expression expr) {
    if (expr instanceof LongValue) {
      return ((LongValue) expr).getValue();
    } else if (expr instanceof StringValue) {
      return ((StringValue) expr).getValue();
    } else if (expr instanceof NullValue) {
      return null;
    } else if (expr instanceof DoubleValue) {
      return ((DoubleValue) expr).getValue();
    } else if (expr instanceof TimestampValue) {
      return ((TimestampValue) expr).getValue();
    } else if (expr instanceof DateValue) {
      return ((DateValue) expr).getValue();
    } else if (expr instanceof TimeValue) {
      return ((TimeValue) expr).getValue();
    } else if (isBooleanLiteral(expr)) {
      String columnName = ((Column) expr).getColumnName();
      // Удаляем кавычки, если они присутствуют
      if (columnName.startsWith("\"") && columnName.endsWith("\"") && columnName.length() > 2) {
        columnName = columnName.substring(1, columnName.length() - 1);
      }
      return Boolean.parseBoolean(columnName.toLowerCase());
    } else {
      throw new IllegalArgumentException(
          "Unsupported expression type: " + expr.getClass()
      );
    }
  }
}
