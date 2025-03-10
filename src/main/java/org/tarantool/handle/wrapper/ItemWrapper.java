package org.tarantool.handle.wrapper;

import static org.tarantool.handle.wrapper.ExpressionWrapper.processExpression;
import static org.tarantool.handle.wrapper.NameWrapper.wrapTableName;
import static org.tarantool.handle.wrapper.RequestTypeWrapper.processSubSelect;

import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ItemWrapper {

  /**
   * Wraps SELECT items.
   *
   * @param selectItems The SELECT items.
   */
  public static void wrapSelectItems(List<SelectItem> selectItems) {
    if (selectItems != null) {
      for (SelectItem item : selectItems) {
        item.accept(new SelectItemVisitorAdapter() {
          @Override
          public void visit(SelectExpressionItem selectExpressionItem) {
            processExpression(selectExpressionItem.getExpression());
          }
        });
      }
    }
  }

  /**
   * Wraps FROM items.
   *
   * @param fromItem The FROM item.
   */
  public static void wrapFromItem(FromItem fromItem) {
    if (fromItem != null) {
      if (fromItem instanceof Table) {
        wrapTableName((Table) fromItem);
      } else if (fromItem instanceof SubSelect) {
        SubSelect subSelect = (SubSelect) fromItem;
        if (subSelect.getSelectBody() != null) {
          processSubSelect(subSelect);
        }
      }
      // Handle other FromItem types if necessary
    }
  }

  /**
   * Wraps JOIN items.
   *
   * @param joinItems The JOIN items.
   */
  public static void wrapJoinItems(List<Join> joinItems) {
    if (joinItems != null) {
      for (Join join : joinItems) {
        wrapFromItem(join.getRightItem());
        if (join.getOnExpression() != null) {
          processExpression(join.getOnExpression());
        }
      }
    }
  }

  /**
   * Wraps WHERE item.
   *
   * @param expression The WHERE item.
   */
  public static void wrapWhere(Expression expression) {
    if (expression != null) {
      processExpression(expression);
    }
  }

  /**
   * Wraps ORDER BY items.
   *
   * @param orderByElements The ORDER BY items.
   */
  public static void wrapOrderByElements(List<OrderByElement> orderByElements) {
    if (orderByElements != null) {
      for (OrderByElement orderByElement : orderByElements) {
        processExpression(orderByElement.getExpression());
      }
    }
  }
}