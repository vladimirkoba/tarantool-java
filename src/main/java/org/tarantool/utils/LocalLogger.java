package org.tarantool.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LocalLogger {

  private static final String LOG_FILE_PATH = "C:/vtb/driver_log.txt";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  /**
   * Логгирует сообщение в файл C:/vtb/driver_log.txt.
   *
   * @param message Сообщение для логгирования
   */
  public static void log(String message) {
    try (FileWriter fileWriter = new FileWriter(LOG_FILE_PATH, true); // Открываем файл в режиме добавления
        PrintWriter printWriter = new PrintWriter(fileWriter)) {

      String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
      printWriter.printf("[%s] %s%n", timestamp, message);

    } catch (IOException e) {
      System.err.printf("Не удалось записать лог: %s%n", e.getMessage());
    }
  }
}
