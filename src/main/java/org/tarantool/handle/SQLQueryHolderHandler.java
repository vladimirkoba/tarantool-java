package org.tarantool.handle;

import static org.tarantool.handle.SQLParameterMapper.mapParameters;
import static org.tarantool.handle.SpaceNameExtractor.extractSpaceName;
import static org.tarantool.logging.LocalLogger.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.tarantool.jdbc.SQLQueryHolder;
import org.tarantool.schema.TarantoolSchemaMeta;
import org.tarantool.schema.TarantoolSpaceMeta;
import org.tarantool.schema.TarantoolSpaceMeta.SpaceField;

public class SQLQueryHolderHandler {

  private final TarantoolSchemaMeta schemaMeta;

  public SQLQueryHolderHandler(TarantoolSchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  public SQLQueryHolder handle(SQLQueryHolder query) {
    if (query.getParams().isEmpty()) {
      query = PreparedStatementConverter.convertSqlToPreparedStatementFormat(query.getQuery());
      log("query after prepared statement modifier: {0}", query.getQuery());
      log("\n");
      log("params after prepared statement modifier: {0}", query.getParams());
    }
    String spaceName = extractSpaceName(query.getQuery());
    Map<String, List<Integer>> sqlParameterNameToPosition = mapParameters(query.getQuery());
    log("Sql parameter name to position map: {0}", sqlParameterNameToPosition);
    Set<String> sqlParams = sqlParameterNameToPosition.keySet();
    log("Space name from table: {0}", spaceName);
    if (spaceName == null) {
      log("Space is null, maybe connection test query");
      return query;
    }
    TarantoolSpaceMeta space = schemaMeta.getSpace(spaceName);
    log("Space types");
    for (SpaceField field : space.getFormat()) {
      log("Space field: {0} type: {1}", field.getName(), field.getType());
    }
    for (SpaceField field : space.getFormat()) {
      if (spaceTypeIsUuid(field) && queryHasSpaceField(field, sqlParams)) {
        replaceStringUuidToProperUuid(query, field, sqlParameterNameToPosition);
        printChangedParameters(query);
      }
      if (spaceTypeIsArray(field) && queryHasSpaceField(field, sqlParams)) {
        replaceStringArrayToProperArray(query, field, sqlParameterNameToPosition);
        printChangedParameters(query);
      }
      if (spaceTypeIsDecimal(field) && queryHasSpaceField(field, sqlParams)) {
        replaceStringDecimalToProperDouble(query, field, sqlParameterNameToPosition);
        printChangedParameters(query);
      }
      if (spaceTypeIsBoolean(field) && queryHasSpaceField(field, sqlParams)) {
        replaceStringBooleanToProperBoolean(query, field, sqlParameterNameToPosition);
        printChangedParameters(query);
      }
    }
    return query;
  }

  private void printChangedParameters(SQLQueryHolder query) {
    log("Changed params:");
    for (Object param : query.getParams()) {
      if (param == null) {
        log("param is null");
        continue;
      }
      log("param = {0} type = {1}", param, param.getClass());
    }
  }

  private void replaceStringArrayToProperArray(SQLQueryHolder query, SpaceField field, Map<String, List<Integer>> sqlParameterNameToPosition) {
    log("sqlParam = {0}", field.getName());
    List<Integer> positions = sqlParameterNameToPosition.get(field.getName());
    if (positions != null) {
      for (Integer position : positions) {
        Object potentialArray = query.getParams().get(position - 1);
        log("potential array at position {0} = {1}", position, potentialArray);
        if (potentialArray != null) {
          String arrayString = potentialArray.toString().trim();
          if (arrayString.equals("{}")) {
            log("Array is empty, setting to null");
            query.getParams().set(position - 1, null);
          } else {
            // Remove the curly braces
            if (arrayString.startsWith("{") && arrayString.endsWith("}")) {
              String content = arrayString.substring(1, arrayString.length() - 1).trim();
              List<Object> array = new ArrayList<>();
              if (!content.isEmpty()) {
                String[] elements = content.split(",");
                for (String element : elements) {
                  element = element.trim();
                  if (element.startsWith("'") && element.endsWith("'") && element.length() >= 2) {
                    element = element.substring(1, element.length() - 1);
                  }
                  array.add(element);
                }
              }
              log("converted array = {0}", array);
              query.getParams().set(position - 1, array);
            } else {
              log("Invalid array format: {0}", arrayString);
            }
          }
        }
      }
    }
  }

  private void replaceStringUuidToProperUuid(SQLQueryHolder query, SpaceField field, Map<String, List<Integer>> sqlParameterNameToPosition) {
    log("sqlParam = {0}", field.getName());
    List<Integer> positions = sqlParameterNameToPosition.get(field.getName());
    if (positions != null) {
      for (Integer position : positions) {
        Object potentialUuid = query.getParams().get(position - 1);
        log("potential uuid at position {0} = {1}", position, potentialUuid);
        if (potentialUuid != null) {
          UUID uuid = UUID.fromString(potentialUuid.toString());
          log("converted uuid = {0}", uuid);
          query.getParams().set(position - 1, uuid);
        }
      }
    }
  }

  private void replaceStringDecimalToProperDouble(SQLQueryHolder query, SpaceField field, Map<String, List<Integer>> sqlParameterNameToPosition) {
    log("sqlParam = {0}", field.getName());
    List<Integer> positions = sqlParameterNameToPosition.get(field.getName());
    if (positions != null) {
      for (Integer position : positions) {
        Object potentialDecimal = query.getParams().get(position - 1);
        log("potential decimal at position {0} = {1}", position, potentialDecimal);
        if (potentialDecimal != null) {
          Double decimalValue = Double.valueOf(potentialDecimal.toString());
          log("converted decimal = {0}", decimalValue);
          query.getParams().set(position - 1, decimalValue);
        }
      }
    }
  }

  private boolean queryHasSpaceField(SpaceField field, Set<String> sqlParams) {
    return sqlParams.contains(field.getName());
  }

  private boolean spaceTypeIsArray(SpaceField field) {
    return field.getType().equals("array");
  }

  private boolean spaceTypeIsUuid(SpaceField field) {
    return field.getType().equals("uuid");
  }

  private boolean spaceTypeIsDecimal(SpaceField field) {
    return field.getType().equals("decimal");
  }

  // Новый метод для преобразования строк в булевы значения
  // В методе replaceStringBooleanToProperBoolean добавляем проверки типов
  private void replaceStringBooleanToProperBoolean(SQLQueryHolder query, SpaceField field,
      Map<String, List<Integer>> sqlParameterNameToPosition) {
    log("sqlParam = {0}", field.getName());
    List<Integer> positions = sqlParameterNameToPosition.get(field.getName());
    if (positions != null) {
      for (Integer position : positions) {
        Object potentialBoolean = query.getParams().get(position - 1);
        log("potential boolean at position {0} = {1}", position, potentialBoolean);

        if (potentialBoolean == null) {
          continue; // Сохраняем null как есть
        }

        if (potentialBoolean instanceof Boolean) {
          log("Parameter is already Boolean: {0}", potentialBoolean);
          continue;
        }

        if (potentialBoolean instanceof String) {
          Boolean booleanValue = Boolean.parseBoolean(((String) potentialBoolean).toLowerCase());
          log("converted boolean = {0}", booleanValue);
          query.getParams().set(position - 1, booleanValue);
        } else {
          log("Invalid boolean type: {0}", potentialBoolean.getClass().getName());
          throw new IllegalArgumentException("Boolean parameter must be String or Boolean. Actual type: "
              + potentialBoolean.getClass().getName());
        }
      }
    }
  }

  // Проверка типа поля на boolean
  private boolean spaceTypeIsBoolean(SpaceField field) {
    return "boolean".equalsIgnoreCase(field.getType());
  }
}