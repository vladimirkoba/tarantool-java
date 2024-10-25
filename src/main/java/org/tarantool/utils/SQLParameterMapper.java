package org.tarantool.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

public class SQLParameterMapper {

  /**
   * Extracts parameters from an SQL query (INSERT, UPDATE, DELETE) with placeholders '?'
   * and returns a Map where the key is the parameter (column) name without quotes,
   * and the value is a list of its positions.
   *
   * @param sqlQuery SQL query of type INSERT, UPDATE, or DELETE with placeholders '?'
   * @return Map<String, List<Integer>> with parameter names and their positions
   */
  public static Map<String, List<Integer>> mapParameters(String sqlQuery) {
    Map<String, List<Integer>> paramMap = new LinkedHashMap<>();
    try {
      Statement statement = CCJSqlParserUtil.parse(sqlQuery);
      if (statement instanceof Insert) {
        handleInsert((Insert) statement, paramMap);
      } else if (statement instanceof Update) {
        handleUpdate((Update) statement, paramMap);
      } else if (statement instanceof Delete) {
        handleDelete((Delete) statement, paramMap);
      } else {
        throw new IllegalArgumentException("Unsupported SQL command. Only INSERT, UPDATE, DELETE are supported.");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return paramMap;
  }

  /**
   * Handles INSERT statements and populates paramMap.
   *
   * @param insert   Insert object from JSqlParser
   * @param paramMap Map to populate
   */
  private static void handleInsert(Insert insert, Map<String, List<Integer>> paramMap) {
    List<Column> columns = insert.getColumns();
    if (columns == null || columns.isEmpty()) {
      throw new IllegalArgumentException("INSERT statement must specify columns.");
    }

    // Extract values from the VALUES part
    if (insert.getItemsList() instanceof ExpressionList) {
      ExpressionList exprList = (ExpressionList) insert.getItemsList();
      List<Expression> expressions = exprList.getExpressions();

      if (columns.size() != expressions.size()) {
        throw new IllegalArgumentException("Number of columns and placeholders do not match.");
      }

      int paramIndex = 1;
      for (int i = 0; i < columns.size(); i++) {
        Expression expr = expressions.get(i);
        if (expr instanceof JdbcParameter) {
          String columnName = stripQuotes(columns.get(i).getColumnName());
          paramMap.computeIfAbsent(columnName, k -> new ArrayList<>()).add(paramIndex++);
        } else {
          paramIndex++; // Still increment index even if not a JdbcParameter
        }
      }
    } else {
      throw new IllegalArgumentException("Unsupported INSERT items list type.");
    }
  }

  /**
   * Handles UPDATE statements and populates paramMap.
   *
   * @param update   Update object from JSqlParser
   * @param paramMap Map to populate
   */
  private static void handleUpdate(Update update, Map<String, List<Integer>> paramMap) {
    List<Column> columns = update.getColumns();
    List<Expression> expressions = update.getExpressions();

    if (columns.size() != expressions.size()) {
      throw new IllegalArgumentException("Number of columns and expressions in SET do not match.");
    }

    int paramIndex = 1;
    // Handle SET part
    for (int i = 0; i < columns.size(); i++) {
      Expression expr = expressions.get(i);
      if (expr instanceof JdbcParameter) {
        String columnName = stripQuotes(columns.get(i).getColumnName());
        paramMap.computeIfAbsent(columnName, k -> new ArrayList<>()).add(paramIndex++);
      } else {
        paramIndex++; // Still increment index even if not a JdbcParameter
      }
    }

    // Handle WHERE part
    Expression where = update.getWhere();
    if (where != null) {
      paramIndex = extractColumnsFromExpression(where, paramMap, paramIndex);
    }
  }

  /**
   * Handles DELETE statements and populates paramMap.
   *
   * @param delete   Delete object from JSqlParser
   * @param paramMap Map to populate
   */
  private static void handleDelete(Delete delete, Map<String, List<Integer>> paramMap) {
    Expression where = delete.getWhere();
    if (where != null) {
      int paramIndex = 1;
      extractColumnsFromExpression(where, paramMap, paramIndex);
    }
  }

  /**
   * Recursively extracts column names from the WHERE expression that are associated with JdbcParameter.
   *
   * @param expr      WHERE expression
   * @param paramMap  Map to populate with column names and positions
   * @param paramIndex Current parameter index
   * @return Updated parameter index after processing the expression
   */
  private static int extractColumnsFromExpression(Expression expr, Map<String, List<Integer>> paramMap, int paramIndex) {
    if (expr instanceof BinaryExpression) {
      BinaryExpression be = (BinaryExpression) expr;
      Expression left = be.getLeftExpression();
      Expression right = be.getRightExpression();

      // Check if left is Column and right is JdbcParameter
      if (left instanceof Column && right instanceof JdbcParameter) {
        String columnName = stripQuotes(((Column) left).getColumnName());
        paramMap.computeIfAbsent(columnName, k -> new ArrayList<>()).add(paramIndex++);
      } else if (right instanceof Column && left instanceof JdbcParameter) {
        // Handle cases where JdbcParameter is on the left
        String columnName = stripQuotes(((Column) right).getColumnName());
        paramMap.computeIfAbsent(columnName, k -> new ArrayList<>()).add(paramIndex++);
      } else {
        // Recursively process left and right expressions
        paramIndex = extractColumnsFromExpression(left, paramMap, paramIndex);
        paramIndex = extractColumnsFromExpression(right, paramMap, paramIndex);
      }
    } else if (expr instanceof Parenthesis) {
      Parenthesis parenthesis = (Parenthesis) expr;
      paramIndex = extractColumnsFromExpression(parenthesis.getExpression(), paramMap, paramIndex);
    }
    // Handle other expression types if necessary
    return paramIndex;
  }

  /**
   * Removes quotes around a column name if present.
   *
   * @param name Column name with possible quotes
   * @return Column name without quotes
   */
  private static String stripQuotes(String name) {
    if ((name.startsWith("\"") && name.endsWith("\"")) ||
        (name.startsWith("`") && name.endsWith("`")) ||
        (name.startsWith("'") && name.endsWith("'"))) {
      return name.substring(1, name.length() - 1);
    }
    return name;
  }
}
