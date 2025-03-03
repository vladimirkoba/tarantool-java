package org.tarantool.handle.wrapper;

import static org.tarantool.handle.wrapper.NameWrapper.wrapColumnName;
import static org.tarantool.handle.wrapper.RequestTypeWrapper.processSubSelect;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionWrapper {

  /**
   * Processes expressions by visiting their components and wrapping column names.
   *
   * @param expression The expression.
   */
  public static void processExpression(Expression expression) {
    if (expression == null) {
      return;
    }

    if (expression instanceof LikeExpression) {
      LikeExpression likeExpr = (LikeExpression) expression;
      processExpression(likeExpr.getLeftExpression());
      processExpression(likeExpr.getRightExpression());
    } else if (expression instanceof InExpression) {
      InExpression inExpr = (InExpression) expression;
      processExpression(inExpr.getLeftExpression());
      processExpression(inExpr.getRightExpression());
    } else if (expression instanceof BinaryExpression) {
      BinaryExpression binaryExpr = (BinaryExpression) expression;
      processExpression(binaryExpr.getLeftExpression());
      processExpression(binaryExpr.getRightExpression());
    } else if (expression instanceof Parenthesis) {
      Parenthesis parenthesis = (Parenthesis) expression;
      processExpression(parenthesis.getExpression());
    } else if (expression instanceof Column) {
      wrapColumnName((Column) expression);
    } else if (expression instanceof SubSelect) {
      processSubSelect(expression);
    } else {
      // Process any nested expressions
      expression.accept(new ExpressionVisitorAdapter() {
        @Override
        public void visit(Column column) {
          wrapColumnName(column);
        }
      });
    }
  }
}