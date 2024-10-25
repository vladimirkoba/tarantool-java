package org.tarantool.jdbc;

import static org.tarantool.util.JdbcConstants.DatabaseMetadataTable;
import static org.tarantool.util.JdbcConstants.DatabaseMetadataTable.INDEX_INFO;
import static org.tarantool.utils.LocalLogger.log;

import org.tarantool.SqlProtoUtils;
import org.tarantool.Version;
import org.tarantool.jdbc.type.TarantoolSqlType;
import org.tarantool.util.TupleTwo;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SQLDatabaseMetadata implements DatabaseMetaData {

  protected static final int _VSPACE = 281;
  protected static final int _VINDEX = 289;
  protected static final int SPACES_MAX = 65535;
  public static final int FORMAT_IDX = 6;
  public static final int NAME_IDX = 2;
  public static final int INDEX_FORMAT_IDX = 5;
  public static final int SPACE_ID_IDX = 0;
  protected final SQLConnection connection;

  public SQLDatabaseMetadata(SQLConnection connection) {
    this.connection = connection;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    log("getProcedures");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.STORED_PROCEDURES);
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    log("allProceduresAreCallable");
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    log("allTablesAreSelectable");
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    log("getURL");
    return connection.getUrl();
  }

  @Override
  public String getUserName() throws SQLException {
    log("getUserName");
    return connection.getProperties().getProperty("user");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    log("isReadOnly");
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    log("nullsAreSortedHigh");
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    log("nullsAreSortedLow");
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    log("nullsAreSortedAtStart");
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    log("nullsAreSortedAtEnd");
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    log("getDatabaseProductName");
    return "Tarantool";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    log("getDatabaseProductVersion");
    return connection.getServerVersion();
  }

  @Override
  public String getDriverName() throws SQLException {
    log("getDriverName");
    return SQLConstant.DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    log("getDriverVersion");
    return Version.version;
  }

  @Override
  public int getDriverMajorVersion() {
    log("getDriverMajorVersion");
    return Version.majorVersion;
  }

  @Override
  public int getDriverMinorVersion() {
    log("getDriverMinorVersion");
    return Version.minorVersion;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    log("usesLocalFiles");
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    log("usesLocalFilePerTable");
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    log("supportsMixedCaseIdentifiers");
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    log("storesUpperCaseIdentifiers");
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    log("storesLowerCaseIdentifiers");
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    log("storesMixedCaseIdentifiers");
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    log("supportsMixedCaseQuotedIdentifiers");
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    log("storesUpperCaseQuotedIdentifiers");
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    log("storesLowerCaseQuotedIdentifiers");
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    log("storesMixedCaseQuotedIdentifiers");
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    log("getIdentifierQuoteString");
    return " ";
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    log("getSQLKeywords");
    return "";
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    log("getNumericFunctions");
    return "";
  }

  @Override
  public String getStringFunctions() throws SQLException {
    log("getStringFunctions");
    return "";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    log("getSystemFunctions");
    return "";
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    log("getTimeDateFunctions");
    return "";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    log("getSearchStringEscape");
    return null;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    log("getExtraNameCharacters");
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    log("supportsAlterTableWithAddColumn");
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    log("supportsAlterTableWithDropColumn");
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    log("supportsColumnAliasing");
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    log("nullPlusNonNullIsNull");
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    log("supportsConvert");
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    log("supportsConvert");
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    log("supportsTableCorrelationNames");
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    log("supportsDifferentTableCorrelationNames");
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    log("supportsExpressionsInOrderBy");
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    log("supportsOrderByUnrelated");
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    log("supportsGroupBy");
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    log("supportsGroupByUnrelated");
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    log("supportsGroupByBeyondSelect");
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    log("supportsLikeEscapeClause");
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    log("supportsMultipleResultSets");
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    log("supportsMultipleTransactions");
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    log("supportsNonNullableColumns");
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    log("supportsMinimumSQLGrammar");
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    log("supportsCoreSQLGrammar");
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    log("supportsExtendedSQLGrammar");
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    log("supportsANSI92EntryLevelSQL");
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    log("supportsANSI92IntermediateSQL");
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    log("supportsANSI92FullSQL");
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    log("supportsIntegrityEnhancementFacility");
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    log("supportsOuterJoins");
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    log("supportsFullOuterJoins");
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    log("supportsLimitedOuterJoins");
    return true;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    log("getSchemaTerm");
    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    log("getProcedureTerm");
    return "procedure";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    log("getCatalogTerm");
    return "catalog";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    log("isCatalogAtStart");
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    log("getCatalogSeparator");
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    log("supportsSchemasInDataManipulation");
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    log("supportsSchemasInProcedureCalls");
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    log("supportsSchemasInTableDefinitions");
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    log("supportsSchemasInIndexDefinitions");
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    log("supportsSchemasInPrivilegeDefinitions");
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    log("supportsCatalogsInDataManipulation");
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    log("supportsCatalogsInProcedureCalls");
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    log("supportsCatalogsInTableDefinitions");
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    log("supportsCatalogsInIndexDefinitions");
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    log("supportsCatalogsInPrivilegeDefinitions");
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    log("supportsPositionedDelete");
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    log("supportsPositionedUpdate");
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    log("supportsSelectForUpdate");
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    log("supportsStoredProcedures");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    log("supportsSubqueriesInComparisons");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    log("supportsSubqueriesInExists");
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    log("supportsSubqueriesInIns");
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    log("supportsSubqueriesInQuantifieds");
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    log("supportsCorrelatedSubqueries");
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    log("supportsUnion");
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    log("supportsUnionAll");
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    log("supportsOpenCursorsAcrossCommit");
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    log("supportsOpenCursorsAcrossRollback");
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    log("supportsOpenStatementsAcrossCommit");
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    log("supportsOpenStatementsAcrossRollback");
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    log("getMaxBinaryLiteralLength");
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    log("getMaxCharLiteralLength");
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    log("getMaxColumnNameLength");
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    log("getMaxColumnsInGroupBy");
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    log("getMaxColumnsInIndex");
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    log("getMaxColumnsInOrderBy");
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    log("getMaxColumnsInSelect");
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    log("getMaxColumnsInTable");
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    log("getMaxConnections");
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    log("getMaxCursorNameLength");
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    log("getMaxIndexLength");
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    log("getMaxSchemaNameLength");
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    log("getMaxProcedureNameLength");
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    log("getMaxCatalogNameLength");
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    log("getMaxRowSize");
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    log("doesMaxRowSizeIncludeBlobs");
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    log("getMaxStatementLength");
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    log("getMaxStatements");
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    log("getMaxTableNameLength");
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    log("getMaxTablesInSelect");
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    log("getMaxUserNameLength");
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    log("getDefaultTransactionIsolation");
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    log("supportsTransactions");
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    log("supportsTransactionIsolationLevel");
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    log("supportsDataDefinitionAndDataManipulationTransactions");
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    log("supportsDataManipulationTransactionsOnly");
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    log("dataDefinitionCausesTransactionCommit");
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    log("dataDefinitionIgnoredInTransactions");
    return false;
  }

  @Override
  public ResultSet getProcedureColumns(String catalog,
      String schemaPattern,
      String procedureNamePattern,
      String columnNamePattern)
      throws SQLException {
    log("getProcedureColumns");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.STORED_PROCEDURE_COLUMNS);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    log("getTables");
    try {
      if (types != null && !Arrays.asList(types).contains("TABLE")) {
        connection.checkNotClosed();
        return asEmptyMetadataResultSet(DatabaseMetadataTable.TABLES);
      }
      String[] parts = tableNamePattern == null ? new String[]{""} : tableNamePattern.split("%");
      List<List<Object>> spaces = (List<List<Object>>) connection.nativeSelect(
          _VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0
      );
      List<List<Object>> rows = new ArrayList<List<Object>>();
      for (List<Object> space : spaces) {
        String tableName = (String) space.get(NAME_IDX);
        List<Map<String, Object>> format = (List<Map<String, Object>>) space.get(FORMAT_IDX);
        /*
         * Skip Tarantool system spaces (started with underscore).
         * Skip spaces that don't have format. Tarantool/SQL does not support such spaces.
         */
        if (!tableName.startsWith("_") && format.size() > 0 && like(tableName, parts)) {
          rows.add(Arrays.asList(
              null, null,
              tableName,
              "TABLE",
              null, null,
              null, null,
              null, null)
          );
        }
      }
      return asMetadataResultSet(DatabaseMetadataTable.TABLES, rows);
    } catch (Exception e) {
      throw new SQLException(
          "Failed to retrieve table(s) description: " +
              "tableNamePattern=\"" + tableNamePattern + "\".", e);
    }
  }

  protected boolean like(String value, String[] parts) {
    log("like");
    if (parts == null || parts.length == 0) {
      return true;
    }
    int i = 0;
    for (String part : parts) {
      i = value.indexOf(part, i);
      if (i < 0) {
        break;
      }
      i += part.length();
    }
    return (i > -1 && (parts[parts.length - 1].length() == 0 || i == value.length()));
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    log("getTableTypes");
    return asMetadataResultSet(
        DatabaseMetadataTable.TABLE_TYPES,
        Collections.singletonList(Arrays.asList("TABLE"))
    );
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    log("getSchemas");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.SCHEMAS);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    log("getSchemas");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.SCHEMAS);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    log("getCatalogs");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.CATALOGS);
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    log("getBestRowIdentifier");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.BEST_ROW_IDENTIFIER);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    log("getColumns");
    try {
      // Разделение шаблонов таблицы и столбцов
      String[] tableParts = tableNamePattern == null ? new String[]{""} : tableNamePattern.split("%");
      String[] colParts = columnNamePattern == null ? new String[]{""} : columnNamePattern.split("%");

      // Получение списка всех таблиц
      List<List<Object>> spaces = (List<List<Object>>) connection.nativeSelect(
          _VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0
      );

      List<List<Object>> rows = new ArrayList<>();

      for (List<Object> space : spaces) {
        String tableName = (String) space.get(NAME_IDX);
        List<Map<String, Object>> format = (List<Map<String, Object>>) space.get(FORMAT_IDX);
        log("Map from space:" + format);
        /*
         * Пропускаем системные пространства Tarantool (начинаются с подчеркивания).
         * Пропускаем пространства без формата, так как Tarantool/SQL не поддерживает их.
         */
        if (!tableName.startsWith("_") && format.size() > 0 && like(tableName, tableParts)) {
          // Получаем первичные ключи для текущей таблицы
          ResultSet primaryKeys = getPrimaryKeys(catalog, schemaPattern, tableName);
          Set<String> primaryKeyColumns = new HashSet<>();

          while (primaryKeys.next()) {
            primaryKeyColumns.add(primaryKeys.getString("COLUMN_NAME"));
          }

          // Обрабатываем формат таблицы
          for (int columnIdx = 1; columnIdx <= format.size(); columnIdx++) {
            Map<String, Object> f = format.get(columnIdx - 1);
            String columnName = (String) f.get("name");
            String typeName = (String) f.get("type");

            // Проверяем имя столбца
            if (like(columnName, colParts)) {
              // Если столбец является первичным ключом, ставим columnNullable, иначе - columnNullableUnknown
              int nullableStatus = primaryKeyColumns.contains(columnName) ? columnNoNulls : columnNullableUnknown;

              rows.add(Arrays.asList(
                  null, // table catalog
                  null, // table schema
                  tableName,
                  columnName,
                  mapToJdbcType(typeName), // data type
                  typeName,
                  null, // column size
                  null, // buffer length
                  null, // decimal digits
                  10, // num prec radix
                  nullableStatus, // nullable статус (если ключ - то columnNullable)
                  null, // remarks
                  null, // column def
                  null, // sql data type
                  null, // sql datatype sub
                  null, // char octet length
                  columnIdx, // ordinal position
                  "", // is nullable
                  null, // scope catalog
                  null, // scope schema
                  null, // scope table
                  Types.OTHER, // source data type
                  "NO", // is autoincrement
                  "NO"  // is generated column
              ));
            }
          }
        }
      }

      return asMetadataResultSet(DatabaseMetadataTable.COLUMNS, rows);
    } catch (Exception e) {
      throw new SQLException(
          "Error processing table column metadata: " +
              "tableNamePattern=\"" + tableNamePattern + "\"; " +
              "columnNamePattern=\"" + columnNamePattern + "\".", e);
    }
  }

  private int mapToJdbcType(String typeName) {
    switch (typeName) {
      case "integer":
        return Types.INTEGER;
      case "double":
        return Types.DOUBLE;
      case "float":
        return Types.FLOAT;
      case "bigint":
        return Types.BIGINT;
      case "numeric":
        return Types.BIGINT;
      case "varchar":
        return Types.VARCHAR;
      case "text":
        return Types.VARCHAR;
      case "uuid":
        return Types.VARCHAR;
      case "unsigned":
        return Types.BIGINT;
      case "boolean":
        return Types.BIT;
      case "bool":
        return Types.BIT;
    }
    return Types.VARCHAR;
  }


  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    log("getColumnPrivileges");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.COLUMN_PRIVILEGES);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    log("getTablePrivileges");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.TABLE_PRIVILEGES);
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    log("getVersionColumns");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.VERSION_COLUMNS);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    log("getImportedKeys");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.FOREIGN_KEYS);
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    log("getPrimaryKeys");
    if (table == null || table.isEmpty()) {
      log("Table is null or empty, returning empty metadata ResultSet.");
      connection.checkNotClosed();
      return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
    }

    try {
      log("Fetching spaces for table: " + table);
      List spaces = connection.nativeSelect(_VSPACE, 2, Collections.singletonList(table), 0, 1, 0);

      if (spaces == null || spaces.size() == 0) {
        log("No spaces found for table: " + table + ", returning empty metadata ResultSet.");
        return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
      }

      log("Processing space data for table: " + table);
      List space = ensureType(List.class, spaces.get(0));
      List fields = ensureType(List.class, space.get(FORMAT_IDX));
      int spaceId = ensureType(Number.class, space.get(SPACE_ID_IDX)).intValue();

      log("Fetching indexes for spaceId: " + spaceId);
      List indexes = connection.nativeSelect(_VINDEX, 0, Arrays.asList(spaceId, 0), 0, 1, 0);

      if (indexes == null || indexes.size() == 0) {
        log("No indexes found for spaceId: " + spaceId + ", returning empty metadata ResultSet.");
        return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
      }

      log("Processing primary key data for spaceId: " + spaceId);
      List primaryKey = ensureType(List.class, indexes.get(0));
      log("PKeys: " + primaryKey);
      List parts = ensureType(List.class, primaryKey.get(INDEX_FORMAT_IDX));
      log("Parts: " + parts);

      List<List<Object>> rows = new ArrayList<>();
      log("Iterating over parts to retrieve column details.");
      for (int i = 0; i < parts.size(); i++) {
        Object partObj = parts.get(i);
        log("parts.get(i) = " + partObj);
        log("parts.get(i).class = " + partObj.getClass());

        if (partObj instanceof Map) {
          Map part = (Map) partObj;
          int ordinal = ensureType(Number.class, part.get("field")).intValue();
          Map field = ensureType(Map.class, fields.get(ordinal));
          String column = ensureType(String.class, field.get("name"));
          rows.add(Arrays.asList(null, null, table, column, i + 1, primaryKey.get(NAME_IDX)));
        } else if (partObj instanceof List) {
          List partList = (List) partObj;
          if (partList.size() >= 1) {
            int ordinal = ensureType(Number.class, partList.get(0)).intValue();
            Map field = ensureType(Map.class, fields.get(ordinal));
            String column = ensureType(String.class, field.get("name"));
            rows.add(Arrays.asList(null, null, table, column, i + 1, primaryKey.get(NAME_IDX)));
          } else {
            log("Invalid partList, returning empty metadata ResultSet.");
            return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
          }
        } else {
          log("Unknown part type, returning empty metadata ResultSet.");
          return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
        }
      }

      log("Sorting result rows by column name.");
      rows.sort((left, right) -> {
        String col0 = (String) left.get(3);
        String col1 = (String) right.get(3);
        return col0.compareTo(col1);
      });

      log("Returning metadata ResultSet rows: " + rows);
      return asMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS, rows);
    } catch (Exception e) {
      log("Error occurred while processing metadata for table \"" + table + "\": " + e.getMessage());
      throw new SQLException("Error processing metadata for table \"" + table + "\".", e);
    }
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    log("getExportedKeys");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.FOREIGN_KEYS);
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    log("getCrossReference");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.FOREIGN_KEYS);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    log("getTypeInfo");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.TYPE_INFO);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    log("getIndexInfo");

    // Получение первичных ключей
    ResultSet primaryKeys = getPrimaryKeys(catalog, schema, table);

    // Сбор информации об индексе на основе первичного ключа
    List<List<Object>> rows = new ArrayList<>();

    while (primaryKeys.next()) {
      String columnName = primaryKeys.getString("COLUMN_NAME");

      // Строим уникальный индекс на основе первичных ключей
      List<Object> row = new ArrayList<>();
      row.add(null);                   // TABLE_CAT
      row.add(null);                   // TABLE_SCHEM
      row.add(table);                  // TABLE_NAME
      row.add(unique);                // NON_UNIQUE
      row.add(null);                   // INDEX_QUALIFIER
      row.add("PRIMARY");              // INDEX_NAME
      row.add(3);                      // TYPE (3 - Clustered index)
      row.add(1);                      // ORDINAL_POSITION (обычно 1 для первичных ключей)
      row.add(columnName);             // COLUMN_NAME
      row.add(null);                   // ASC_OR_DESC
      row.add(0);                      // CARDINALITY (Можно указать как 0, если не известно)
      row.add(0);                      // PAGES (Можно указать как 0, если не известно)
      row.add(null);                   // FILTER_CONDITION

      rows.add(row);
    }
    return asMetadataResultSet(INDEX_INFO, rows);
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    log("getUDTs");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.UDTS);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    log("supportsResultSetType");
    return type == ResultSet.TYPE_FORWARD_ONLY ||
        type == ResultSet.TYPE_SCROLL_INSENSITIVE;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    log("supportsResultSetConcurrency");
    return supportsResultSetType(type) && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    log("ownUpdatesAreVisible");
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    log("ownDeletesAreVisible");
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    log("ownInsertsAreVisible");
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    log("othersUpdatesAreVisible");
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    log("othersDeletesAreVisible");
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    log("othersInsertsAreVisible");
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    log("updatesAreDetected");
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    log("deletesAreDetected");
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    log("insertsAreDetected");
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    log("supportsBatchUpdates");
    return true;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    log("getSuperTypes");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.SUPER_TYPES);
  }

  @Override
  public Connection getConnection() throws SQLException {
    log("getConnection");
    return connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    log("supportsSavepoints");
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    log("supportsNamedParameters");
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    log("supportsMultipleOpenResults");
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    log("supportsGetGeneratedKeys");
    return true;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    log("getSuperTables");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.SUPER_TABLES);
  }

  @Override
  public ResultSet getAttributes(String catalog,
      String schemaPattern,
      String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    log("getAttributes");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.ATTRIBUTES);
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    log("getClientInfoProperties");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.CLIENT_INFO_PROPERTIES);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    log("supportsResultSetHoldability");
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    log("getResultSetHoldability");
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    log("getDatabaseMajorVersion");
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    log("getDatabaseMinorVersion");
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    log("getJDBCMajorVersion");
    return 2;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    log("getJDBCMinorVersion");
    return 1;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    log("getSQLStateType");
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    log("locatorsUpdateCopy");
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    log("supportsStatementPooling");
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    log("getRowIdLifetime");
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    log("supportsStoredFunctionsUsingCallSyntax");
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    log("autoCommitFailureClosesAllResultSets");
    return false;
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    log("getFunctions");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.FUNCTIONS);
  }

  @Override
  public ResultSet getFunctionColumns(String catalog,
      String schemaPattern,
      String functionNamePattern,
      String columnNamePattern)
      throws SQLException {
    log("getFunctionColumns");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.FUNCTION_COLUMNS);
  }

  @Override
  public ResultSet getPseudoColumns(String catalog,
      String schemaPattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    log("getPseudoColumns");
    return asEmptyMetadataResultSet(DatabaseMetadataTable.PSEUDO_COLUMNS);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    log("generatedKeyAlwaysReturned");
    return true;
  }

  @Override
  public <T> T unwrap(Class<T> type) throws SQLException {
    log("unwrap");
    if (isWrapperFor(type)) {
      return type.cast(this);
    }
    throw new SQLNonTransientException("SQLDatabaseMetadata does not wrap " + type.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> type) throws SQLException {
    log("isWrapperFor");
    return type.isAssignableFrom(this.getClass());
  }

  private TarantoolStatement createMetadataStatement() throws SQLException {
    log("createMetadataStatement");
    return connection.createStatement().unwrap(TarantoolStatement.class);
  }

  private static <T> T ensureType(Class<T> cls, Object v) throws Exception {
    log("ensureType");
    if (v == null || !cls.isAssignableFrom(v.getClass())) {
      throw new Exception(String.format("Wrong value type '%s', expected '%s'.",
          v == null ? "null" : v.getClass().getName(), cls.getName()));
    }
    return cls.cast(v);
  }

  private static <T> T checkType(Class<T> cls, Object v) {
    log("checkType");
    return (v != null && cls.isAssignableFrom(v.getClass())) ? cls.cast(v) : null;
  }

  private ResultSet asMetadataResultSet(List<TupleTwo<String, TarantoolSqlType>> meta, List<List<Object>> rows)
      throws SQLException {
    log("asMetadataResultSet");
    List<SqlProtoUtils.SQLMetaData> sqlMeta = meta.stream()
        .map(tuple -> new SqlProtoUtils.SQLMetaData(tuple.getFirst(), tuple.getSecond()))
        .collect(Collectors.toList());
    SQLResultHolder holder = SQLResultHolder.ofQuery(sqlMeta, rows);
    return createMetadataStatement().executeMetadata(holder);
  }

  private ResultSet asEmptyMetadataResultSet(List<TupleTwo<String, TarantoolSqlType>> meta) throws SQLException {
    log("asEmptyMetadataResultSet");
    return asMetadataResultSet(meta, Collections.emptyList());
  }
}
