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
   * Converts a SQL query into a parameterized format suitable for prepared statements. If the query is already parameterized (contains '?'), it
   * returns the query as is. Otherwise, it replaces literal values with '?' and collects the values into params.
   *
   * @param sql The SQL query to convert.
   * @return An SQLQueryHolder containing the parameterized query and parameters.
   */
  public static SQLQueryHolder convertSqlToPreparedStatementFormat(String sql) {
    List<Object> params = new ArrayList<>();
    try {
      Statement statement = CCJSqlParserUtil.parse(sql);

      if (statement instanceof Insert) {
        Insert insert = (Insert) statement;
        processInsertStatement(insert, params);
        String modifiedSql = insert.toString();
        return SQLQueryHolder.of(modifiedSql, params.toArray()); // Ensure varargs
      } else if (statement instanceof Update) {
        Update update = (Update) statement;
        processUpdateStatement(update, params);
        String modifiedSql = update.toString();
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Delete) {
        Delete delete = (Delete) statement;
        processDeleteStatement(delete, params);
        String modifiedSql = delete.toString();
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else if (statement instanceof Select) {
        // Optionally handle SELECT statements
        Select select = (Select) statement;
        processSelectStatement(select, params);
        String modifiedSql = select.toString();
        return SQLQueryHolder.of(modifiedSql, params.toArray());
      } else {
        throw new IllegalArgumentException("Unsupported SQL command. Only INSERT, UPDATE, DELETE, SELECT are supported.");
      }
    } catch (Exception e) {
      e.printStackTrace();
      return SQLQueryHolder.of(sql); // Return original query with no params on error
    }
  }

  private static void processInsertStatement(Insert insert, List<Object> params) {
    if (insert.getItemsList() instanceof net.sf.jsqlparser.expression.operators.relational.ExpressionList) {
      net.sf.jsqlparser.expression.operators.relational.ExpressionList exprList =
          (net.sf.jsqlparser.expression.operators.relational.ExpressionList) insert.getItemsList();
      List<Expression> expressions = exprList.getExpressions();

      for (int i = 0; i < expressions.size(); i++) {
        Expression expr = expressions.get(i);
        Object value = extractValue(expr);
        params.add(value);
        expressions.set(i, new JdbcParameter());
      }
    } else {
      throw new IllegalArgumentException("Unsupported INSERT items list type.");
    }
  }

  private static void processUpdateStatement(Update update, List<Object> params) {
    List<Expression> expressions = update.getExpressions();

    for (int i = 0; i < expressions.size(); i++) {
      Expression expr = expressions.get(i);
      Object value = extractValue(expr);
      params.add(value);
      expressions.set(i, new JdbcParameter());
    }

    // Process WHERE clause
    Expression where = update.getWhere();
    if (where != null) {
      update.setWhere(processExpression(where, params));
    }
  }

  private static void processDeleteStatement(Delete delete, List<Object> params) {
    // Process WHERE clause
    Expression where = delete.getWhere();
    if (where != null) {
      delete.setWhere(processExpression(where, params));
    }
  }

  private static void processSelectStatement(Select select, List<Object> params) {
    if (select.getSelectBody() instanceof PlainSelect) {
      PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
      Expression where = plainSelect.getWhere();
      if (where != null) {
        plainSelect.setWhere(processExpression(where, params));
      }
    }
  }

  private static Expression processExpression(Expression expr, List<Object> params) {
    if (expr instanceof BinaryExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expr;
      Expression leftExpr = binaryExpr.getLeftExpression();
      Expression rightExpr = binaryExpr.getRightExpression();

      binaryExpr.setLeftExpression(processExpression(leftExpr, params));
      binaryExpr.setRightExpression(processExpression(rightExpr, params));
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

  private static boolean isLiteralValue(Expression expr) {
    return expr instanceof LongValue ||
        expr instanceof StringValue ||
        expr instanceof NullValue ||
        expr instanceof DoubleValue ||
        expr instanceof TimestampValue ||
        expr instanceof DateValue ||
        expr instanceof TimeValue ||
        expr instanceof Column; // Included Column
  }

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
    } else if (expr instanceof Column) {
      // Handle 'null', 'true', 'false' as special cases
      String columnName = ((Column) expr).getColumnName();
      if (columnName.equalsIgnoreCase("null")) {
        return null;
      } else if (columnName.equalsIgnoreCase("true")) {
        return Boolean.TRUE;
      } else if (columnName.equalsIgnoreCase("false")) {
        return Boolean.FALSE;
      } else {
        throw new IllegalArgumentException("Unsupported column in value position: " + columnName);
      }
    } else {
      throw new IllegalArgumentException("Unsupported expression type: " + expr.getClass());
    }
  }
}
