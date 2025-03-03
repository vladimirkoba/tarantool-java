package org.tarantool.handle.util;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalDecoder {

  /**
   * Декодирует массив байт Tarantool DECIMAL в BigDecimal.
   *
   * @param data байтовый массив, полученный из MsgPack EXT type=1
   * @return BigDecimal представление числа
   */
  public static BigDecimal decode(byte[] data) {
    if (data.length < 2) {
      throw new IllegalArgumentException("Invalid decimal data length");
    }

    // Читаем flags (масштаб)
    byte flags = data[0];
    int scale = Byte.toUnsignedInt(flags);

    // Читаем знак из последнего байта
    byte signByte = data[data.length - 1];
    boolean negative;
    if ((signByte & 0x0F) == 0x0C) {
      negative = false;
    } else if ((signByte & 0x0F) == 0x0D) {
      negative = true;
    } else {
      throw new IllegalArgumentException("Unknown sign nibble: " + String.format("0x%02X", signByte));
    }

    return new BigDecimal(getBigInteger(data, signByte, negative), scale);
  }

  private static BigInteger getBigInteger(byte[] data, byte signByte, boolean negative) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < data.length - 1; i++) {
      byte b = data[i];
      int highNibble = (b & 0xF0) >> 4;
      int lowNibble = b & 0x0F;
      sb.append(highNibble);
      sb.append(lowNibble);
    }

    // Добавляем цифру из high nibble последнего байта
    int lastHighNibble = (signByte & 0xF0) >> 4;
    sb.append(lastHighNibble);

    String digitsStr = sb.toString().replaceFirst("^0+(?!$)", ""); // Убираем ведущие нули, но оставляем хотя бы одну цифру

    BigInteger unscaled = new BigInteger(digitsStr);
    if (negative) {
      unscaled = unscaled.negate();
    }
    return unscaled;
  }
}