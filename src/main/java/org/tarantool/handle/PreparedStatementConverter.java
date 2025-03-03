// file: src/main/java/org/tarantool/utils/PreparedStatementConverter.java
package org.tarantool.handle;

import static org.tarantool.logging.LocalLogger.errorLog;
import static org.tarantool.logging.LocalLogger.log;
import static org.tarantool.util.StringUtils.stripQuotes;

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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
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
   * Converts a SQL query into a parameterized format suitable for prepared statements. If the query already contains '?', returns it unchanged.
   * Otherwise, it replaces literal values with '?' and collects them into params.
   */
  public static SQLQueryHolder convertSqlToPreparedStatementFormat(String sql) {
    List<Object> params = new ArrayList<>();
    log("[PreparedStatementConverter] Original SQL: {0}", sql);
    try {
      Statement statement = CCJSqlParserUtil.parse(sql);
      log("[PreparedStatementConverter] Parsed Statement: {0}", statement.getClass().getSimpleName());

      if (statement instanceof Insert) {
        log("[PreparedStatementConverter] Processing INSERT statement.");
        Insert insert = (Insert) statement;
        processInsertStatement(insert, params);
        String modifiedSql = insert.toString();
        log("[PreparedStatementConverter] Modified SQL: {0}", modifiedSql);
        log("[PreparedStatementConverter] Parameters: {0}", params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Update) {
        log("[PreparedStatementConverter] Processing UPDATE statement.");
        Update update = (Update) statement;
        processUpdateStatement(update, params);
        String modifiedSql = update.toString();
        log("[PreparedStatementConverter] Modified SQL: {0}", modifiedSql);
        log("[PreparedStatementConverter] Parameters: {0}", params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Delete) {
        log("[PreparedStatementConverter] Processing DELETE statement.");
        Delete delete = (Delete) statement;
        processDeleteStatement(delete, params);
        String modifiedSql = delete.toString();
        log("[PreparedStatementConverter] Modified SQL: {0}", modifiedSql);
        log("[PreparedStatementConverter] Parameters: {0}", params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Select) {
        log("[PreparedStatementConverter] Processing SELECT statement.");
        Select select = (Select) statement;
        processSelectStatement(select, params);
        String modifiedSql = select.toString();
        log("[PreparedStatementConverter] Modified SQL: {0}", modifiedSql);
        log("[PreparedStatementConverter] Parameters: {0}", params);
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else {
        throw new IllegalArgumentException("Unsupported SQL command. Only INSERT, UPDATE, DELETE, SELECT are supported.");
      }
    } catch (Exception e) {
      errorLog(e, "[PreparedStatementConverter] Error parsing SQL.");
      return SQLQueryHolder.of(sql);
    }
  }

  private static void processInsertStatement(Insert insert, List<Object> params) {
    log("[PreparedStatementConverter] Entering processInsertStatement.");
    if (insert.getItemsList() instanceof ExpressionList) {
      ExpressionList exprList = (ExpressionList) insert.getItemsList();
      List<Expression> expressions = exprList.getExpressions();
      log("[PreparedStatementConverter] INSERT expressions count: {0}", expressions.size());
      extractParam(expressions, params);
    } else {
      throw new IllegalArgumentException("Unsupported INSERT items list type.");
    }
    log("[PreparedStatementConverter] Exiting processInsertStatement.");
  }

  private static void processUpdateStatement(Update update, List<Object> params) {
    log("[PreparedStatementConverter] Entering processUpdateStatement.");
    List<Expression> expressions = update.getExpressions();
    log("[PreparedStatementConverter] UPDATE expressions count: {0}", expressions.size());
    extractParam(expressions, params);
    Expression where = update.getWhere();
    if (where != null) {
      update.setWhere(processExpression(where, params));
    }
    log("[PreparedStatementConverter] Exiting processUpdateStatement.");
  }

  private static void processDeleteStatement(Delete delete, List<Object> params) {
    log("[PreparedStatementConverter] Entering processDeleteStatement.");
    Expression where = delete.getWhere();
    if (where != null) {
      delete.setWhere(processExpression(where, params));
    }
    log("[PreparedStatementConverter] Exiting processDeleteStatement.");
  }

  private static void processSelectStatement(Select select, List<Object> params) {
    log("[PreparedStatementConverter] Entering processSelectStatement.");
    if (select.getSelectBody() instanceof PlainSelect) {
      PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
      Expression where = plainSelect.getWhere();
      if (where != null) {
        plainSelect.setWhere(processExpression(where, params));
      }
    }
    log("[PreparedStatementConverter] Exiting processSelectStatement.");
  }

  private static Expression processExpression(Expression expr, List<Object> params) {
    if (expr instanceof IsNullExpression || expr instanceof IsBooleanExpression) {
      return expr;
    } else if (expr instanceof BinaryExpression) {
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
    return expr instanceof LongValue
        || expr instanceof StringValue
        || expr instanceof NullValue
        || expr instanceof DoubleValue
        || expr instanceof TimestampValue
        || expr instanceof DateValue
        || expr instanceof TimeValue
        || isBooleanLiteral(expr); // Обрабатываем булевые литералы, представленные как Column("true") или Column("false")
  }

  /**
   * Checks if the expression is a boolean literal (true/false) при виде "Column('false')" вместо нативного BooleanValue(false).
   */
  private static boolean isBooleanLiteral(Expression expr) {
    if (expr instanceof Column) {
      String columnName = stripQuotes(((Column) expr).getColumnName());
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
      String columnName = stripQuotes(((Column) expr).getColumnName());
      return Boolean.parseBoolean(columnName.toLowerCase());
    } else {
      throw new IllegalArgumentException("Unsupported expression type: " + expr.getClass());
    }
  }

  private static void extractParam(List<Expression> expressions, List<Object> params) {
    for (int i = 0; i < expressions.size(); i++) {
      Expression expr = expressions.get(i);
      if (isLiteralValue(expr)) {
        Object value = extractValue(expr);
        params.add(value);
        expressions.set(i, new JdbcParameter());
      }
    }
  }
}