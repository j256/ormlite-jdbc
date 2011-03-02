package com.j256.ormlite.db;

/**
 * Generic JdbcOdbcBridge database type information used to create the tables, etc..
 * 
 * @author Dale Asberry
 */
public class GenericOdbcDatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "odbc";
	private final static String DRIVER_CLASS_NAME = "sun.jdbc.odbc.JdbcOdbcDriver";
	private final static String DATABASE_NAME = "ODBC";

	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String getDriverClassName() {
		return DRIVER_CLASS_NAME;
	}

	@Override
	protected String getDatabaseName() {
		return DATABASE_NAME;
	}
}
