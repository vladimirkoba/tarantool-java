package org.tarantool.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class LocalLogger {

  /**
   * Временно отключен
   * @param message
   */
  public static void log(String message) {
    String timeStamp = java.time.LocalDateTime.now().toString();
    String logMessage = timeStamp + " - " + message;
    try (BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\vtb\\driverLog.txt", true))) {
      writer.write(logMessage);
      writer.newLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
