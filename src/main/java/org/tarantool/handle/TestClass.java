package org.tarantool.handle;

import static org.tarantool.handle.wrapper.RequestWrapper.wrap;

public class TestClass {

  public static void main(String[] args) {
    // Test Case 1: Simple SELECT
    String sql1 = "SELECT id, name, age FROM users";
    String expected1 = "SELECT \"id\", \"name\", \"age\" FROM \"users\"";
    String result1 = wrap(sql1);
    assert result1.equals(expected1) : "Test Case 1 Failed";

    // Test Case 2: SELECT with ORDER BY
    String sql2 = "SELECT id, name FROM employees ORDER BY name DESC, id ASC";
    String expected2 = "SELECT \"id\", \"name\" FROM \"employees\" ORDER BY \"name\" DESC, \"id\" ASC";
    String result2 = wrap(sql2);
    assert result2.equals(expected2) : "Test Case 2 Failed";

    // Test Case 3: SELECT with LIKE ? ESCAPE '#'
    String sql3 = "SELECT * FROM products WHERE description LIKE ? ESCAPE '#'";
    String expected3 = "SELECT * FROM \"products\" WHERE \"description\" = ?";
    String result3 = wrap(sql3);
    assert result3.equals(expected3) : "Test Case 3 Failed";

    // Test Case 4: INSERT Statement with Aliases
    String sql4 = "INSERT INTO orders (order_id, customer_name) VALUES (?, ?)";
    String expected4 = "INSERT INTO \"orders\" (\"order_id\", \"customer_name\") VALUES (?, ?)";
    String result4 = wrap(sql4);
    assert result4.equals(expected4) : "Test Case 4 Failed";

    // Test Case 5: UPDATE Statement with Aliases and WHERE clause
    String sql5 = "UPDATE employees e SET e.salary = ? WHERE e.id = ?";
    String expected5 = "UPDATE \"employees\" SET \"salary\" = ? WHERE \"id\" = ?";
    String result5 = wrap(sql5);
    assert result5.equals(expected5) : "Test Case 5 Failed";

    // Test Case 6: DELETE Statement with WHERE clause
    String sql6 = "DELETE FROM sessions s WHERE s.user_id = ?";
    String expected6 = "DELETE FROM \"sessions\" WHERE \"user_id\" = ?";
    String result6 = wrap(sql6);
    assert result6.equals(expected6) : "Test Case 6 Failed";

    // Test Case 7: SELECT with JOIN and LIKE ? ESCAPE '#'
    String sql7 = "SELECT u.id, u.name, o.order_date FROM users u JOIN orders o ON u.id = o.user_id WHERE o.description LIKE ? ESCAPE '#'";
    String expected7 = "SELECT \"id\", \"name\", \"order_date\" FROM \"users\" JOIN \"orders\" ON \"id\" = \"user_id\" WHERE \"description\" = ?";
    String result7 = wrap(sql7);
    assert result7.equals(expected7) : "Test Case 7 Failed";

    // Test Case 8: NULL SQL
    String sql8 = null;
    String expected8 = null;
    String result8 = wrap(sql8);
    assert result8 == expected8 : "Test Case 8 Failed";

    // Test Case 9: Empty SQL
    String sql9 = "   ";
    String expected9 = "   ";
    String result9 = wrap(sql9);
    assert result9.equals(expected9) : "Test Case 9 Failed";

    // Test Case 10: Complex WHERE Clause with Multiple Conditions
    String sql10 = "SELECT * FROM inventory WHERE category = ? AND name LIKE ? ESCAPE '#' OR quantity > ?";
    String expected10 = "SELECT * FROM \"inventory\" WHERE \"category\" = ? AND \"name\" = ? OR \"quantity\" > ?";
    String result10 = wrap(sql10);
    assert result10.equals(expected10) : "Test Case 10 Failed";

    // Test Case 11: UPDATE with LIKE ? ESCAPE '#'
    String sql11 = "UPDATE products SET name = ? WHERE description LIKE ? ESCAPE '#'";
    String expected11 = "UPDATE \"products\" SET \"name\" = ? WHERE \"description\" = ?";
    String result11 = wrap(sql11);
    assert result11.equals(expected11) : "Test Case 11 Failed";

    // Test Case 12: DELETE with LIKE ? ESCAPE '#'
    String sql12 = "DELETE FROM orders WHERE order_code LIKE ? ESCAPE '#'";
    String expected12 = "DELETE FROM \"orders\" WHERE \"order_code\" = ?";
    String result12 = wrap(sql12);
    assert result12.equals(expected12) : "Test Case 12 Failed";

    // Test Case 13: UPDATE with multiple LIKE ? ESCAPE '#' conditions
    String sql13 = "UPDATE security_type SET version = ? WHERE id = ? AND bucket_id = ? AND version LIKE ? ESCAPE '#' AND name LIKE ? ESCAPE '#' AND type LIKE ? ESCAPE '#' AND cat_name IS NULL";
    String expected13 = "UPDATE \"security_type\" SET \"version\" = ? WHERE \"id\" = ? AND \"bucket_id\" = ? AND \"version\" = ? AND \"name\" = ? AND \"type\" = ? AND \"cat_name\" IS NULL";
    String result13 = wrap(sql13);
    assert result13.equals(expected13) : "Test Case 13 Failed";

    //todo Дописать тесты для IS FALSE, now(), JOIN, SubSelect, Group by

    System.out.println("All test cases passed successfully!");
  }
}