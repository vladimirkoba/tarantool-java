package org.tarantool.utils;

import static org.tarantool.utils.LocalLogger.log;
import static org.tarantool.utils.SpaceNameExtractor.extractSpaceName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.tarantool.jdbc.SQLQueryHolder;
import org.tarantool.schema.TarantoolSchemaMeta;
import org.tarantool.schema.TarantoolSpaceMeta;
import org.tarantool.schema.TarantoolSpaceMeta.SpaceField;

public class UuidReplacer {

  private TarantoolSchemaMeta schemaMeta;

  public UuidReplacer(TarantoolSchemaMeta schemaMeta) {
    this.schemaMeta = schemaMeta;
  }

  public SQLQueryHolder updateUuidToProperType(SQLQueryHolder query) {
    if (query.getParams().isEmpty()) {
      query = PreparedStatementConverter.convertSqlToPreparedStatementFormat(query.getQuery());
      log("query after prepared statement modifier:" + query.getQuery());
      log("\n");
      log("params after prepared statement modifier:" + query.getParams());
    }
    String spaceName = extractSpaceName(query.getQuery());
    Map<String, List<Integer>> sqlParameterNameToPosition = SQLParameterMapper.mapParameters(query.getQuery());
    log("Sql parameter name to position map:" + sqlParameterNameToPosition);
    Set<String> sqlParams = sqlParameterNameToPosition.keySet();
    log("Space name from table: " + spaceName);
    if (spaceName == null) {
      log("Space is null, maybe connection test query");
      return query;
    }
    TarantoolSpaceMeta space = schemaMeta.getSpace(spaceName);
    log("Space types");
    for (SpaceField field : space.getFormat()) {
      log("Space field:" + field.getName() + " type:" + field.getType());
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
      log("param = " + param + " type = " + param.getClass());
    }
  }

  private void replaceStringArrayToProperArray(SQLQueryHolder query, SpaceField field, Map<String, List<Integer>> sqlParameterNameToPosition) {
    log("sqlParam = " + field.getName());
    List<Integer> positions = sqlParameterNameToPosition.get(field.getName());
    if (positions != null) {
      for (Integer position : positions) {
        Object potentialArray = query.getParams().get(position - 1);
        log("potential array at position " + position + " = " + potentialArray);
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
              log("converted array = " + array);
              query.getParams().set(position - 1, array);
            } else {
              log("Invalid array format: " + arrayString);
            }
          }
        }
      }
    }
  }


  private void replaceStringUuidToProperUuid(SQLQueryHolder query, SpaceField field, Map<String, List<Integer>> sqlParameterNameToPosition) {
    log("sqlParam = " + field.getName());
    List<Integer> positions = sqlParameterNameToPosition.get(field.getName());
    if (positions != null) {
      for (Integer position : positions) {
        Object potentialUuid = query.getParams().get(position - 1);
        log("potential uuid at position " + position + " = " + potentialUuid);
        if (potentialUuid != null) {
          UUID uuid = UUID.fromString(potentialUuid.toString());
          log("converted uuid = " + uuid);
          query.getParams().set(position - 1, uuid);
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

}
