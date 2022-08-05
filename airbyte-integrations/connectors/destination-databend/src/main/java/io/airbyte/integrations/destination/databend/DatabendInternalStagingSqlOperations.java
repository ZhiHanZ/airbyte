package io.airbyte.integrations.destination.databend;
import io.airbyte.commons.string.Strings;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.integrations.destination.staging.StagingOperations;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseStatement;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.joda.time.DateTime;

public class DatabendInternalStagingSqlOperations extends DatabendSqlOperations implements StagingOperations {
    private static final Integer MAX_RETRY = 5;
    private static final int MAX_FILES_IN_LOADING_QUERY_LIMIT = 1000;
    private static final String CREATE_STAGE_QUERY = "CREATE STAGE IF NOT EXISTS %s;";
    private static final String DROP_STAGE_QUERY = "DROP STAGE IF EXISTS %s;";
    private static final String DELETE_STAGE_OBJECT_QUERY = "DELETE @%s/%s;";
    private static final String PRESIGN_UPLOAD_QUERY = "PRESIGN UPLOAD @%s/%s%s;";
    private static final String PRESIGN_DOWNLOAD_QUERY = "PRESIGN @%s/%s%s;";
    private static final String REMOVE_QUERY = "REMOVE @%s;";

    private static final String COPY_INTO_QUERY = "COPY INTO %s.%s FROM @%s/%s %s file_format = (type = 'csv' compression = auto );";
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabendInternalStagingSqlOperations.class);

    private final NamingConventionTransformer nameTransformer;
  
    public DatabendInternalStagingSqlOperations(final NamingConventionTransformer nameTransformer) {
      this.nameTransformer = nameTransformer;
    }

    @Override
    public String getStageName(String namespace, String streamName) {
        return nameTransformer.applyDefaultCase(String.join("_", nameTransformer.convertStreamName(namespace), 
        nameTransformer.convertStreamName(streamName)));
    }

    @Override
    public String getStagingPath(UUID connectionId, String namespace, String streamName, DateTime writeDatetime) {
        return nameTransformer.applyDefaultCase(String.format("%s/%02d/%02d/%02d/%s/",
        writeDatetime.year().get(),
        writeDatetime.monthOfYear().get(),
        writeDatetime.dayOfMonth().get(),
        writeDatetime.hourOfDay().get(),
        connectionId));
    }

    @Override
    public void createStageIfNotExists(final JdbcDatabase database, final String stageName) throws Exception {
        final String query = getCreateStageQuery(stageName);
        LOGGER.debug("Executing query: {}", query);
        database.execute(query);
    }

    protected String getCreateStageQuery(final String stageName) {
        return String.format(CREATE_STAGE_QUERY, stageName);
    }
    
    @Override
    public void dropStageIfExists(final JdbcDatabase database, final String stageName) throws Exception {
      final String query = getDropQuery(stageName);
      LOGGER.debug("Executing query: {}", query);
      database.execute(query);
    }
  
    protected String getDropQuery(final String stageName) {
      return String.format(DROP_STAGE_QUERY, stageName);
    }

    protected String getPresignStageUploadQuery(final String stageName, final String stagingPath, final String fileName) {
        return String.format(PRESIGN_UPLOAD_QUERY, stageName, stagingPath, fileName);
    }

    protected void putFileIntoStage(final JdbcDatabase database, final String stageName, final String stagingPath, final SerializableBuffer record) throws Exception {
        String path;
        String fileName;
        try {
            path = record.getFile().getAbsolutePath();
            fileName = record.getFile().getName();
        } catch (IOException e){
            throw new Exception(String.format("failed to get file path with error %s", e.getMessage()));
        }
        if (path.isEmpty() || path.isBlank() || fileName.isEmpty() || fileName.isBlank()) {
            throw new Exception(String.format("record file path is empty"));
        }
        database.execute(connection -> {
            ClickHouseConnection conn = connection.unwrap(ClickHouseConnection.class);
            ClickHouseStatement sth = conn.createStatement();
            
            final String query = getPresignStageUploadQuery(stageName, stagingPath, fileName);
            LOGGER.debug("Executing query: {}", query);
            
            
            final var rs = sth.executeQuery(query);
            while (rs.next()) {
                String method = rs.getString("method");
                String headers = rs.getString("headers");
                String url = rs.getString("url");
                
                LOGGER.debug("Method: {}, Headers: {}, URL: {}", method, headers, url);
                File file = new File(path);
                try (InputStream in = new FileInputStream(file)) {
                    Map<String, String> head = DatabendUtils.getHeaders(headers);
                    int code = uploadFile(head, url, in);
                    if (code >= 400 || code < 200) {
                        throw new SQLException(String.format("Upload failed with code %d in url %s", code, url));        
                    } else {
                        LOGGER.debug("successful upload of file {} into @%s/%s", path, stageName, stagingPath);
                    }
                } catch (SQLException e) {
                    throw e;
                } catch (IOException e) {
                    throw new SQLException(String.format("Upload failed with Exception %s", e.getMessage()));        
                } catch (Exception e) {
                    throw new SQLException(String.format("Upload failed with Exception %s", e.getMessage()));        
                }
                
            }
            rs.close();
        });
    }
    protected String generateFilesList(final List<String> files) {
        if (0 < files.size() && files.size() < MAX_FILES_IN_LOADING_QUERY_LIMIT) {
          // see https://docs.snowflake.com/en/user-guide/data-load-considerations-load.html#lists-of-files
          final StringJoiner joiner = new StringJoiner(",");
          files.forEach(filename -> joiner.add("'" + filename.substring(filename.lastIndexOf("/") + 1) + "'"));
          return " files = (" + joiner + ")";
        } else {
          return "";
        }
    }

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
                + "%s Timestamp DEFAULT now(),\n"
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

    protected String getCopyQuery(final String stageName, final String stagingPath,
                                  final List<String> stagedFiles, final String dstTableName, final String schemaName) {
        return String.format(COPY_INTO_QUERY, schemaName, dstTableName, stageName, stagingPath, generateFilesList(stagedFiles));
    }
    private static int uploadFile(Map<String, String> headers, String url, InputStream content) throws Exception{
        HttpPut req = new HttpPut(url);
        // set headers 
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            req.setHeader(entry.getKey(), entry.getValue());
        }
        
        InputStreamEntity contentEntity = new InputStreamEntity(content, -1);
        req.setEntity(contentEntity);
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // Put the file on storage using the presigned url
        URIBuilder builder = new URIBuilder(req.getURI());

        req.setURI(builder.build());

        CloseableHttpResponse response = httpClient.execute(req);
        
        return response.getStatusLine().getStatusCode();
    }

    protected String getPresignStageDownloadQuery(final String stageName, final String stagingPath) {
        return String.format(PRESIGN_DOWNLOAD_QUERY, stageName, stagingPath);
    }


    @Override
    public String uploadRecordsToStage(JdbcDatabase database, SerializableBuffer recordsData, String schemaName,
            String stageName, String stagingPath) throws Exception {
        final List<Exception> exceptionsThrown = new ArrayList<>();
        boolean succeeded = false;
        while (exceptionsThrown.size() < MAX_RETRY && !succeeded) {
            try {
                putFileIntoStage(database, stageName, stagingPath, recordsData);
                succeeded = true;
            } catch (final Exception e) {
                LOGGER.error("Failed to upload records into stage {}", stagingPath, e);
                exceptionsThrown.add(e);
            }
            if (!succeeded) {
                LOGGER.info("Retrying to upload records into stage {} ({}/{}})", stagingPath, exceptionsThrown.size(), MAX_RETRY);
            }
        }
        if (!succeeded) {
            throw new RuntimeException(
                String.format("Exceptions thrown while uploading records into stage: %s", Strings.join(exceptionsThrown, "\n")));
        }
        return recordsData.getFilename();
    }
    @Override
    public void copyIntoTmpTableFromStage(JdbcDatabase database, String stageName, String stagingPath,
            List<String> stagedFiles, String dstTableName, String schemaName) throws Exception {
        // TODO Auto-generated method stub
        final String query = getCopyQuery(stageName, stagingPath, stagedFiles, dstTableName, schemaName);
        LOGGER.debug("Executing query: {}", query);
        database.execute(query);
    }

    @Override
    public void cleanUpStage(final JdbcDatabase database, final String stageName, final List<String> stagedFiles) throws Exception {
      final String query = getRemoveQuery(stageName);
      LOGGER.debug("Executing query: {}", query);
      database.execute(query);
    }
    
    protected String getRemoveQuery(final String stageName) {
        return String.format(REMOVE_QUERY, stageName);
    }
    

}