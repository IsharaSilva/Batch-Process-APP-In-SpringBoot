package com.synergensolutions.sbsservice.reports.reader;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

public class DBReader<T> extends JdbcCursorItemReader<T> {

	public DBReader(DataSource dataSource, String query, RowMapper<T> rowMapper) {
		super();
		setDataSource(dataSource);
		setSql(query);
		setRowMapper(rowMapper);
	}

}
