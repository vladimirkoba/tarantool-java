package org.tarantool.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.ListIterator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.values.ValuesStatement;

public class ReplaceProblemExpression {

  public static Expression checkWhere(Expression expression) {
    Expression expressionAfterFirstCheck = substitutionIsBooleanExpression(expression);
    return substitutionNowFunctionFromWhere(expressionAfterFirstCheck);
  }

  private static Expression substitutionIsBooleanExpression(Expression expression) {
    if (expression instanceof BinaryExpression) {
      BinaryExpression binaryExpression = (BinaryExpression) expression;
      Expression left = substitutionIsBooleanExpression(binaryExpression.getLeftExpression());
      Expression right = substitutionIsBooleanExpression(binaryExpression.getRightExpression());
      binaryExpression.setLeftExpression(left);
      binaryExpression.setRightExpression(right);
    }

    if (expression instanceof IsBooleanExpression) {
      IsBooleanExpression isBool = (IsBooleanExpression) expression;
      if (isBool.isNot()) {
        if (isBool.isTrue()) {
          return new NotEqualsTo(isBool.getLeftExpression(), new Column(Boolean.TRUE.toString()));
        } else {
          return new NotEqualsTo(isBool.getLeftExpression(), new Column(Boolean.FALSE.toString()));
        }
      } else {
        if (isBool.isTrue()) {
          return new EqualsTo(isBool.getLeftExpression(), new Column(Boolean.TRUE.toString()));
        } else {
          return new EqualsTo(isBool.getLeftExpression(), new Column(Boolean.FALSE.toString()));
        }
      }
    } else {
      return expression;
    }
  }

  private static Expression substitutionNowFunctionFromWhere(Expression expression) {
    if (expression instanceof BinaryExpression) {
      BinaryExpression binaryExpression = (BinaryExpression) expression;
      Expression left = substitutionNowFunctionFromWhere(binaryExpression.getLeftExpression());
      Expression right = substitutionNowFunctionFromWhere(binaryExpression.getRightExpression());
      binaryExpression.setLeftExpression(left);
      binaryExpression.setRightExpression(right);
    }

    if (expression instanceof Function) {
      Function function = (Function) expression;
      if ("now".equalsIgnoreCase(function.getName())) {
        return new LongValue(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
      }
    }
    return expression;
  }

  public static Select checkSelect(Select select) {
    return substitutionNowFunctionFromSelect(select);
  }

  private static Select substitutionNowFunctionFromSelect(Select select) {
    SelectBody selectBody = select.getSelectBody();
    selectBody.accept(new SelectVisitorAdapter() {
      @Override
      public void visit(SetOperationList setOpList) {
        List<SelectBody> selects = setOpList.getSelects();
        for (SelectBody selectBody : selects) {
          if (selectBody instanceof ValuesStatement) {
            ValuesStatement valuesStatement = (ValuesStatement) selectBody;
            ItemsList itemsList = valuesStatement.getExpressions();
            if (itemsList instanceof ExpressionList) {
              ExpressionList expressionList = (ExpressionList) itemsList;
              List<Expression> expressions = expressionList.getExpressions();
              for (Expression expression : expressions) {
                if (expression instanceof RowConstructor) {
                  RowConstructor rowConstructor = (RowConstructor) expression;
                  ExpressionList exprList = rowConstructor.getExprList();
                  List<Expression> rowConstructorExpressionList = exprList.getExpressions();
                  ListIterator<Expression> expressionIterator = rowConstructorExpressionList.listIterator();
                  while (expressionIterator.hasNext()) {
                    Expression next = expressionIterator.next();
                    if (next instanceof Function) {
                      Function function = (Function) next;
                      if ("now".equalsIgnoreCase(function.getName())) {
                        expressionIterator.set(new LongValue(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)));
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    });
    return select;
  }
}