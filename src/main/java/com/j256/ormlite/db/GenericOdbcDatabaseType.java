package com.j256.ormlite.db;

/**
 * Generic JdbcOdbcBridge database type information used to create the tables, etc..
 * 
 * <p>
 * <b>NOTE:</b> This is the initial take on this database type. We hope to get access to an external database for
 * testing. Please contact us if you'd like to help with this class.
 * </p>
 * 
 * @author Dale Asberry
 */
public class GenericOdbcDatabaseType extends BaseDatabaseType {

	private final static String DATABASE_URL_PORTION = "odbc";
	private final static String DRIVER_CLASS_NAME = "sun.jdbc.odbc.JdbcOdbcDriver";
	private final static String DATABASE_NAME = "ODBC";

	@Override
	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String getDriverClassName() {
		return DRIVER_CLASS_NAME;
	}

	@Override
	public String getDatabaseName() {
		return DATABASE_NAME;
	}
}
