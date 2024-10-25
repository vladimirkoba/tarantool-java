package org.tarantool.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpaceNameExtractor {

  public static String extractSpaceName(String sqlQuery) {
    // Убираем лишние пробелы
    String normalizedQuery = sqlQuery.trim();

    // Регулярные выражения для различных SQL-запросов
    String selectPattern = "(?i)select\\s+.*\\s+from\\s+([\"`]?\\w+[\"`]?(?:\\.[" + "\"`]?\\w+[\"`]?)?)";
    String insertPattern = "(?i)insert\\s+into\\s+([\"`]?\\w+[\"`]?(?:\\.[" + "\"`]?\\w+[\"`]?)?)";
    String updatePattern = "(?i)update\\s+([\"`]?\\w+[\"`]?(?:\\.[" + "\"`]?\\w+[\"`]?)?)";
    String deletePattern = "(?i)delete\\s+from\\s+([\"`]?\\w+[\"`]?(?:\\.[" + "\"`]?\\w+[\"`]?)?)";

    // Попробуем найти название таблицы в зависимости от типа SQL-запроса
    String tableName = matchTableName(normalizedQuery, selectPattern);
    if (tableName == null) {
      tableName = matchTableName(normalizedQuery, insertPattern);
    }
    if (tableName == null) {
      tableName = matchTableName(normalizedQuery, updatePattern);
    }
    if (tableName == null) {
      tableName = matchTableName(normalizedQuery, deletePattern);
    }

    // Удаляем кавычки, если они присутствуют
    if (tableName != null) {
      tableName = tableName.replaceAll("[\"`]", "");
    }


    return tableName;
  }

  private static String matchTableName(String query, String regex) {
    Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }
}
