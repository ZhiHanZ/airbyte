/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.databend;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.jdbc.JdbcSqlOperations;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;

public class DatabendSqlOperations extends JdbcSqlOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabendSqlOperations.class);

  @Override
  public void createSchemaIfNotExists(final JdbcDatabase database, final String schemaName) throws Exception {
    database.execute(String.format("CREATE DATABASE IF NOT EXISTS %s;\n", schemaName));
  }

  @Override
  public boolean isSchemaRequired() {
    return false;
  }

  @Override
  public String createTableQuery(final JdbcDatabase database, final String schemaName, final String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ( \n"
            + "%s String,\n"
            + "%s JSON,\n"
            + "%s Date DEFAULT now(),\n"
            + ")\n"
            + "CLUSTER BY(%s)",
        schemaName, tableName,
        JavaBaseConstants.COLUMN_NAME_AB_ID,
        JavaBaseConstants.COLUMN_NAME_DATA,
        JavaBaseConstants.COLUMN_NAME_EMITTED_AT,
        JavaBaseConstants.COLUMN_NAME_AB_ID);
  }

  @Override
  public void executeTransaction(final JdbcDatabase database, final List<String> queries) throws Exception {
    final StringBuilder appendedQueries = new StringBuilder();
    for (final String query : queries) {
      appendedQueries.append(query);
    }
    database.execute(appendedQueries.toString());
  }

  protected JsonNode formatData(final JsonNode data) {
    return data;
  }

  protected void writeBatchToFileJSONEACHROW(final File tmpFile, final List<AirbyteRecordMessage> records) throws Exception {
    try (final PrintWriter writer = new PrintWriter(tmpFile, StandardCharsets.UTF_8)) {
      for (final AirbyteRecordMessage record : records) {
        final var uuid = UUID.randomUUID().toString();
        final var jsonData = Jsons.serialize(formatData(record.getData()));
        final var emittedAt = Timestamp.from(Instant.ofEpochMilli(record.getEmittedAt()));
        JSONObject object = new JSONObject();
        object.put(JavaBaseConstants.COLUMN_NAME_AB_ID,uuid);
        object.put(JavaBaseConstants.COLUMN_NAME_DATA, jsonData);
        object.put(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, emittedAt.toString());
        writer.print(object);
        writer.println();
      }
    }
  }
  @Override
  public void insertRecordsInternal(final JdbcDatabase database,
                                    final List<AirbyteRecordMessage> records,
                                    final String schemaName,
                                    final String tmpTableName)
      throws SQLException {
    LOGGER.info("actual size of batch: {}", records.size());

    if (records.isEmpty()) {
      return;
    }

    database.execute(connection -> {
      File tmpFile = null;
      Exception primaryException = null;
      try {
        tmpFile = Files.createTempFile(tmpTableName + "-", ".tmp").toFile();
        writeBatchToFileJSONEACHROW(tmpFile, records);

        ClickHouseConnection conn = connection.unwrap(ClickHouseConnection.class);
        ClickHouseStatement sth = conn.createStatement();
        sth.write() // Write API entrypoint
            .table(String.format("%s.%s", schemaName, tmpTableName)) // where to write data
            .data(tmpFile, ClickHouseFormat.JSONEachRow) // specify input
            .send();

      } catch (final Exception e) {
        primaryException = e;
        throw new RuntimeException(e);
      } finally {
        try {
          if (tmpFile != null) {
            Files.delete(tmpFile.toPath());
          }
        } catch (final IOException e) {
          if (primaryException != null)
            e.addSuppressed(primaryException);
          throw new RuntimeException(e);
        }
      }
    });
  }

}
