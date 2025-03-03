package org.tarantool.util;

public class StringUtils {

  public static boolean isEmpty(String string) {
    return (string == null) || (string.isEmpty());
  }

  public static boolean isNotEmpty(String string) {
    return !isEmpty(string);
  }

  public static boolean isBlank(String string) {
    return (string == null) || (string.trim().isEmpty());
  }

  public static boolean isNotBlank(String string) {
    return !isBlank(string);
  }

  /**
   * Removes quotes around a column name if present.
   *
   * @param string Column or table name with possible quotes
   * @return Column or table name without quotes
   */
  public static String stripQuotes(String string) {
    if (string == null || string.isEmpty()) {
      return string;
    }
    if ((string.startsWith("\"") && string.endsWith("\"")) ||
        (string.startsWith("`") && string.endsWith("`")) ||
        (string.startsWith("'") && string.endsWith("'"))) {
      return string.substring(1, string.length() - 1);
    }
    return string;
  }
}