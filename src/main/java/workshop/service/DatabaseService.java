package workshop.service;

import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import workshop.model.Column;
import workshop.model.Database;
import workshop.model.Table;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Slf4j
@Service
public class DatabaseService {

    private final DataSource sourceDataSource;

    private final DataSource targetDataSource;

    private final HttpSession session;

    @Value("${source.database.schema-name}")
    private String sourceSchemaName;

    @Value("${target.database.schema-name}")
    private String targetSchemaName;

    public DatabaseService(@Qualifier("sourceDataSource") DataSource sourceDataSource, @Qualifier("targetDataSource") DataSource targetDataSource, HttpSession session) {
        this.sourceDataSource = sourceDataSource;
        this.targetDataSource = targetDataSource;
        this.session = session;
    }

    @SuppressWarnings("unchecked")
    public void getSourceDatabaseInfo() {
        try (Connection connection = Objects.requireNonNull(DataSourceUtils.getConnection(sourceDataSource))) {
            DatabaseMetaData metaData = connection.getMetaData();

            if (session.getAttribute("source_username") == null) {
                String userName = metaData.getUserName();
                log.info("source database username: {}", userName);
                session.setAttribute("source_username", userName);
            }

            if (session.getAttribute("source_database_name") == null) {
                String sourceProductName = metaData.getDatabaseProductName();
                String sourceProductNameWithVersion = getProductNameWithVersion(sourceProductName, metaData.getDatabaseMajorVersion(), metaData.getDatabaseMinorVersion());
                log.info("source database name with version: {}", sourceProductNameWithVersion);
                session.setAttribute("source_database_name", sourceProductNameWithVersion);
            }
            if (session.getAttribute("source_service_name") == null) {
                String url = metaData.getURL();
                String serviceName = StringUtils.substringAfterLast(url, "/");
                log.info("source url: {}", url);
                log.info("source database service name: {}", serviceName);
                session.setAttribute("source_service_name", serviceName);
            }

            if (session.getAttribute("source_tables") == null) {
                List<String> sourceTables = new ArrayList<>();
                ResultSet tableResultSet = metaData.getTables(null, sourceSchemaName, null, new String[]{"TABLE"});
                while (tableResultSet.next()) {
                    sourceTables.add(tableResultSet.getString("TABLE_NAME"));
                }
                log.info("total source tables available: {}", sourceTables.size());
                session.setAttribute("source_table_names", sourceTables);
            }

            if (session.getAttribute("database_tables") == null && session.getAttribute("source_tables") != null) {
                List<String> sourceTables = (List<String>) session.getAttribute("source_tables");

                List<Table> tables = new ArrayList<>();
                for (String tableName : sourceTables) {
                    ResultSet columnResultSet = metaData.getColumns(null, sourceSchemaName, tableName, null);

                    List<Column> columns = new ArrayList<>();

                    while (columnResultSet.next()) {
                        String columnName = columnResultSet.getString("COLUMN_NAME");
                        String columnType = columnResultSet.getString("TYPE_NAME");
                        int columnIndex = columnResultSet.getInt("ORDINAL_POSITION");
                        columns.add(new Column(columnName, columnType, columnIndex));
                    }
                    tables.add(new Table(tableName, columns));
                }
                Database db = new Database(tables);
                session.setAttribute("database_tables", db);
            }
            //tables.forEach(table -> log.info(table.toString()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getProductNameWithVersion(String name, int version, int minorVersion) {
        return name + " " + version + "." + minorVersion;
    }

    public List<Map<Column, Object>> getTableRows(Table table, String whereClause) {
        String tableName = table.tableName();

        String sql = StringUtils.EMPTY;
        if (StringUtils.isNotBlank(tableName)) {
            sql = "select * from " + tableName;
        }
        if (StringUtils.isNotBlank(whereClause)) {
            sql += sql + " " + whereClause;
        }
        log.info("select sql query: {}", sql);
        List<Map<Column, Object>> results = new ArrayList<>();

        try (Connection connection = Objects.requireNonNull(DataSourceUtils.getConnection(sourceDataSource))) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            List<Column> columns = table.columns();

            while (rs.next()) {
                var row = new HashMap<Column, Object>();

                for (Column column : columns) {
                    Object obj = rs.getObject(column.columnIndex());
                    row.put(column, obj);
                }
                results.add(row);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info("results size: {}", results.size());
        return results;
    }

    public void persistTableRows(List<Map<Column, Object>> tableRows, Table table) {
        try (Connection connection = Objects.requireNonNull(DataSourceUtils.getConnection(targetDataSource))) {
            DatabaseMetaData metaData = connection.getMetaData();
            String userName = metaData.getUserName();
            String targetProductName = metaData.getDatabaseProductName();
            int targetProductVersion = metaData.getDatabaseMajorVersion();
            int targetProductMinorVersion = metaData.getDatabaseMinorVersion();
            String url = metaData.getURL();
            String serviceName = StringUtils.substringAfterLast(url, "/");

            log.info("target database username: {}", userName);
            log.info("target database name: {}", targetProductName);
            log.info("target database version: {}", targetProductVersion + "." + targetProductMinorVersion);
            log.info("target database service name: {}", serviceName);
            log.info("target url: {}", url);

            String sql = "INSERT INTO " + table.tableName() + " VALUES (" + placeholder(table.columns().size()) + ")";
            log.info("insert sql: {}", sql);

            final PreparedStatement ps = connection.prepareStatement(sql);

            for (Map<Column, Object> map : tableRows) {
                for (Map.Entry<Column, Object> entry : map.entrySet()) {
                    Column key = entry.getKey();
                    Object value = entry.getValue();
                    ps.setObject(key.columnIndex(), value);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String placeholder(int totalColumn) {
        String placeholder = "?, ";
        placeholder = placeholder.repeat(totalColumn).strip();
        return placeholder.substring(0, placeholder.length() - 1);
    }

    public void getSourceDatabaseDetails() {
    }

    public void getTargetDatabaseDetails() {

    }
}
