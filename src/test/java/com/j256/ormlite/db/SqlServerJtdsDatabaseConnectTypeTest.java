package com.j256.ormlite.db;

import java.sql.SQLException;

import com.j256.ormlite.jdbc.JdbcConnectionSource;

public class SqlServerJtdsDatabaseConnectTypeTest extends SqlServerDatabaseTypeTest {

	@Override
	protected void setDatabaseParams() throws SQLException {
		databaseUrl = "jdbc:jtds:sqlserver://db/ormlite;ssl=request";
		connectionSource = new JdbcConnectionSource(DEFAULT_DATABASE_URL);
		databaseType = new SqlServerJtdsDatabaseType();
	}
}
