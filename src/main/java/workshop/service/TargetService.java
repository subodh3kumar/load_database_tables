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
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class TargetService {

    private final Connection connection;

    public TargetService(@Qualifier("targetDataSource") DataSource dataSource) {
        connection = Objects.requireNonNull(DataSourceUtils.getConnection(dataSource));
    }


    public void persistTableRows(List<Map<Column, Object>> tableRows, Table table) {
        try {
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

            //(EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, HIRE_DATE, MANAGER_ID, JOB_TITLE)
            String sql = "INSERT INTO " + table.tableName() + "(EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, HIRE_DATE, MANAGER_ID, JOB_TITLE) VALUES (" + placeholder(table.columns().size()) + ")";
            log.info("insert sql: {}", sql);

            final PreparedStatement ps = connection.prepareStatement(sql);

            for (Map<Column, Object> map : tableRows) {
                for (Map.Entry<Column, Object> entry : map.entrySet()) {
                    Column key = entry.getKey();
                    Object value = entry.getValue();

                    log.info("key: {}", key);
                    log.info("value: {}", value);

                    ps.setObject(key.ordinalPosition(), value);
            }
            log.info("=========================");
            ps.addBatch();
        }
        ps.executeBatch();


            /*for (Map<Column, Object> map : tableRows) {
                map.forEach((key, value) -> {
                    log.info("key: {}", key);
                    log.info("value: {}", value);

                    try {
                        switch (key.columnType()) {
                            case "NUMBER", "INTEGER" -> {
                                log.info("----> {}", value);
                                if (value instanceof Integer integer) {
                                    ps.setInt(key.ordinalPosition(), integer);
                                }
                            }
                            case "DATE", "TIMESTAMP" -> {
                                if (value instanceof Date date) {
                                    Timestamp timestamp = Timestamp.valueOf(String.valueOf(date.getTime()));
                                    ps.setTimestamp(key.ordinalPosition(), timestamp);
                                }
                            }
                            default -> {
                                assert value instanceof String;
                                ps.setString(key.ordinalPosition(), (String) value);
                            }
                        }
                        //ps.addBatch();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                ps.addBatch();
                log.info("============================");
            }*/
        log.info(".......................");
        ps.executeBatch();
    } catch(
    SQLException e)

    {
        throw new RuntimeException(e);
    }

}

    private String placeholder(int totalColumn) {
        String placeholder = "?, ";
        placeholder = placeholder.repeat(totalColumn).strip();
        return placeholder.substring(0, placeholder.length() - 1);
    }
}
