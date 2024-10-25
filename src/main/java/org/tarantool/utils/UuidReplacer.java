package org.tarantool.utils;

import static org.tarantool.utils.LocalLogger.log;
import static org.tarantool.utils.SpaceNameExtractor.extractSpaceName;

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

  public void updateUuidToProperType(SQLQueryHolder query) {
    String spaceName = extractSpaceName(query.getQuery());
    Map<String, List<Integer>> sqlParamToPosition = SQLParameterMapper.mapParameters(query.getQuery());
    log("Sql query after replace");
    Set<String> sqlParams = sqlParamToPosition.keySet();
    log("Space name from table: " + spaceName);
    if (spaceName == null) {
      log("Space is null, maybe connection test query");
      return;
    }
    TarantoolSpaceMeta space = schemaMeta.getSpace(spaceName);
    log("Space types");
    for (SpaceField field : space.getFormat()) {
      log("Space field:" + field.getName() + " type:" + field.getType());
    }
    for (SpaceField field : space.getFormat()) {
      if (spaceTypeIsUuid(field) && queryHasSpaceField(field, sqlParams)) {
        replaceStringUuidToProperUuid(query, field, sqlParamToPosition);
        printChangedParameters(query);
      }
    }
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

  private void replaceStringUuidToProperUuid(SQLQueryHolder query, SpaceField field, Map<String, List<Integer>> sqlParamToPosition) {
    log("sqlParam = " + field.getName());
    List<Integer> positions = sqlParamToPosition.get(field.getName());
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

  private boolean spaceTypeIsUuid(SpaceField field) {
    return field.getType().equals("uuid");
  }

}
