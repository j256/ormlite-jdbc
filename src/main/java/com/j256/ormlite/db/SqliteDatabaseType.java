package com.j256.ormlite.db;

/**
 * Sqlite database type information used to create the tables, etc..
 * 
 * @author graywatson
 */
public class SqliteDatabaseType extends BaseSqliteDatabaseType implements DatabaseType {

	private final static String DATABASE_URL_PORTION = "sqlite";
	private final static String DRIVER_CLASS_NAME = "org.sqlite.JDBC";

	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String getDriverClassName() {
		return DRIVER_CLASS_NAME;
	}
}
