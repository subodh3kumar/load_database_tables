package workshop.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import workshop.model.Column;
import workshop.model.Table;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Slf4j
@Service
public class SourceService {

    private final DataSource dataSource;

    public SourceService(@Qualifier("sourceDataSource") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<Table> getSourceDatabaseInfo() {
        List<Table> tables = new ArrayList<>();

        try (Connection connection = Objects.requireNonNull(DataSourceUtils.getConnection(dataSource))) {
            DatabaseMetaData metaData = connection.getMetaData();

            String userName = metaData.getUserName();
            String sourceProductName = metaData.getDatabaseProductName();
            int sourceProductVersion = metaData.getDatabaseMajorVersion();
            int sourceProductMinorVersion = metaData.getDatabaseMinorVersion();
            String url = metaData.getURL();
            String serviceName = StringUtils.substringAfterLast(url, "/");

            log.info("source database username: {}", userName);
            log.info("source database name: {}", sourceProductName);
            log.info("source database version: {}", sourceProductVersion + "." + sourceProductMinorVersion);
            log.info("source database service name: {}", serviceName);

            List<String> sourceTables = new ArrayList<>();
            ResultSet tableResultSet = metaData.getTables(null, userName, null, new String[]{"TABLE"});
            while (tableResultSet.next()) {
                sourceTables.add(tableResultSet.getString("TABLE_NAME"));
            }
            log.info("total source tables available: {}", sourceTables.size());

            for (String tableName : sourceTables) {
                ResultSet columnResultSet = metaData.getColumns(null, userName, tableName, null);

                List<Column> columns = new ArrayList<>();

                while (columnResultSet.next()) {
                    String columnName = columnResultSet.getString("COLUMN_NAME");
                    String columnType = columnResultSet.getString("TYPE_NAME");
                    int ordinalPosition = columnResultSet.getInt("ORDINAL_POSITION");
                    columns.add(new Column(columnName, columnType, ordinalPosition));
                }
                tables.add(new Table(tableName, columns));
            }
            //tables.forEach(table -> log.info(table.toString()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tables;
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

        try (Connection connection = Objects.requireNonNull(DataSourceUtils.getConnection(dataSource))) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            List<Column> columns = table.columns();

            while (rs.next()) {
                var row = new HashMap<Column, Object>();

                for (Column column : columns) {
                    Object obj = rs.getObject(column.ordinalPosition());
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
}
