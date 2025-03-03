package org.tarantool.logging;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LocalLogger {

  private static final String LOG_FILE_PATH = "C:/tarantool_logs/driver_log.txt";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  /**
   * Логгирует сообщение в файл C:/tarantool_logs/driver_log.txt.
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

  /**
   * Логгирует сообщение с параметрами в файл C:/tarantool_logs/driver_log.txt.
   *
   * @param message Сообщение для логгирования
   * @param args Список параметров для форматирования сообщения для логгирования
   */
  public static void log(String message, Object... args) {
    try (FileWriter fileWriter = new FileWriter(LOG_FILE_PATH, true); // Открываем файл в режиме добавления
        PrintWriter printWriter = new PrintWriter(fileWriter)) {

      String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
      printWriter.printf("[%s] %s%n", timestamp, MessageFormat.format(message, args));

    } catch (IOException e) {
      System.err.printf("Не удалось записать лог: %s%n", e.getMessage());
    }
  }

  /**
   * Логгирует сообщение об ошибке в файл C:/tarantool_logs/driver_log.txt.
   *
   * @param throwable Объект исключения для логгирования
   * @param message Сообщение для логгирования
   */
  public static void errorLog(Throwable throwable, String message) {
    try (FileWriter fileWriter = new FileWriter(LOG_FILE_PATH, true); // Открываем файл в режиме добавления
        PrintWriter printWriter = new PrintWriter(fileWriter)) {

      String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
      printWriter.printf("[%s] %s%n", timestamp, MessageFormat.format(message + " Error message: {}", throwable.getMessage()));
      throwable.printStackTrace(printWriter);
      printWriter.println();

    } catch (IOException e) {
      System.err.printf("Не удалось записать лог: %s%n", e.getMessage());
    }
  }
}