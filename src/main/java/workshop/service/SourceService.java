package workshop.service;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import workshop.model.Column;
import workshop.model.Table;

@Slf4j
@Service
public class SourceService {

	private final JdbcTemplate sourceJdbcTemplate;
	private final JdbcTemplate targetJdbcTemplate;

	public SourceService(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
						 @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate) {
		this.sourceJdbcTemplate = sourceJdbcTemplate;
		this.targetJdbcTemplate = targetJdbcTemplate;
	}

	public void getSourceDatabaseInfo(HttpSession session) {
		try {
			DatabaseMetaData metaData = sourceJdbcTemplate.getDataSource().getConnection().getMetaData();

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
			ResultSet tableResultSet = metaData.getTables(null, userName, null, new String[] { "TABLE" });
			while (tableResultSet.next()) {
				sourceTables.add(tableResultSet.getString("TABLE_NAME"));
			}
			log.info("total source tables available: {}", sourceTables.size());

			List<Table> tables = new ArrayList<>();

			for (String tableName : sourceTables) {
				ResultSet columnResultSet = metaData.getColumns(null, userName, tableName, null);

				List<Column> columns = new ArrayList<>();

				while (columnResultSet.next()) {
					String columnName = columnResultSet.getString("COLUMN_NAME");
					String columnType = columnResultSet.getString("TYPE_NAME");
					columns.add(new Column(columnName, columnType));
				}
				tables.add(new Table(tableName, columns));
			}
			tables.forEach(table -> log.info(table.toString()));

			Optional<Table> findFirstTable = tables.stream()
					.filter(table -> "EMPLOYEE".equalsIgnoreCase(table.tableName())).findFirst();

			if (findFirstTable.isPresent()) {
				Table table = findFirstTable.get();
				log.info("table name: {}", table);
			}

		} catch (SQLException e) {
			log.error("ERROR:", e);
		}
	}

	public void getTableRows() {
		String sql = "select * from ora.emp";

		// List<Map<String, Object>> queryForList =
		// sourceJdbcTemplate.queryForList(sql);

		// queryForList.forEach(obj -> extract(obj));

	}

	public void getTableRows2() {
		String sql = "select * from ora.emp";

		sourceJdbcTemplate.query(sql, new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				// TODO Auto-generated method stub

			}
		});

		sourceJdbcTemplate.query(sql, rs -> {

		});
	}

	private Object extract(Map<String, Object> obj) {
		log.info("----------------------size: {}", obj.size());
		obj.forEach((k, v) -> log.info("key: {}, value: {}", k, (v == null ? "" : v)));
		return null;
	}
}
