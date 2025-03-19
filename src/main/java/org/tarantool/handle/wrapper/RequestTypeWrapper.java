package org.tarantool.handle.wrapper;

import static org.tarantool.handle.wrapper.ExpressionWrapper.processExpression;
import static org.tarantool.handle.wrapper.ItemWrapper.wrapFromItem;
import static org.tarantool.handle.wrapper.ItemWrapper.wrapGroupByElements;
import static org.tarantool.handle.wrapper.ItemWrapper.wrapJoinItems;
import static org.tarantool.handle.wrapper.ItemWrapper.wrapOrderByElements;
import static org.tarantool.handle.wrapper.ItemWrapper.wrapSelectItems;
import static org.tarantool.handle.wrapper.ItemWrapper.wrapWhere;
import static org.tarantool.handle.wrapper.NameWrapper.wrapColumnName;
import static org.tarantool.handle.wrapper.NameWrapper.wrapTableName;
import static org.tarantool.util.ReplaceProblemExpression.checkSelect;
import static org.tarantool.util.ReplaceProblemExpression.checkWhere;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;

public class RequestTypeWrapper {

  /**
   * Processes SELECT statements to wrap table and column names in double quotes.
   *
   * @param select The SELECT statement.
   */
  public static void processSelect(Select select) {
    processSelectBody(select.getSelectBody());
  }

  public static void processSubSelect(Expression expression) {
    processSelectBody(((SubSelect) expression).getSelectBody());
  }

  private static void processSelectBody(SelectBody selectBody) {
    selectBody.accept(new SelectVisitorAdapter() {
      @Override
      public void visit(PlainSelect plainSelect) {
        // Process SELECT items
        wrapSelectItems(plainSelect.getSelectItems());

        // Process FROM item
        wrapFromItem(plainSelect.getFromItem());

        // Process JOINs
        wrapJoinItems(plainSelect.getJoins());

        // Process WHERE clause
        if (plainSelect.getWhere() != null) {
          plainSelect.setWhere(checkWhere(plainSelect.getWhere()));
          wrapWhere(plainSelect.getWhere());
        }

        // Process ORDER BY elements
        wrapOrderByElements(plainSelect.getOrderByElements());

        // Process GROUP BY elements
        wrapGroupByElements(plainSelect.getGroupBy());
      }
    });
  }

  /**
   * Processes INSERT statements to wrap table and column names in double quotes and remove aliases.
   *
   * @param insert The INSERT statement.
   */
  public static void processInsert(Insert insert) {
    // Wrap table name and remove alias
    wrapTableName(insert.getTable());

    // Wrap column names
    if (insert.getColumns() != null) {
      for (Column column : insert.getColumns()) {
        wrapColumnName(column);
      }
    }

    // Process SELECT part if present (INSERT INTO ... SELECT ...)
    if (insert.getSelect() != null) {
      insert.setSelect(checkSelect(insert.getSelect()));
      processSelect(insert.getSelect());
    }
  }

  /**
   * Processes UPDATE statements to wrap table and column names in double quotes, remove aliases.
   *
   * @param update The UPDATE statement.
   */
  public static void processUpdate(Update update) {
    // Wrap table name and remove alias
    wrapTableName(update.getTable());

    // Wrap column names in SET expressions
    if (update.getColumns() != null) {
      for (Column column : update.getColumns()) {
        wrapColumnName(column);
      }
    }

    // Wrap expressions in SET expressions
    if (update.getExpressions() != null) {
      for (Expression expression : update.getExpressions()) {
        processExpression(expression);
      }
    }

    // Process WHERE clause
    if (update.getWhere() != null) {
      update.setWhere(checkWhere(update.getWhere()));
      processExpression(update.getWhere());
    }
  }

  /**
   * Processes DELETE statements to wrap table and column names in double quotes, remove aliases.
   *
   * @param delete The DELETE statement.
   */
  public static void processDelete(Delete delete) {
    // Wrap table name and remove alias
    wrapTableName(delete.getTable());

    // Process WHERE clause
    if (delete.getWhere() != null) {
      delete.setWhere(checkWhere(delete.getWhere()));
      processExpression(delete.getWhere());
    }
  }
}