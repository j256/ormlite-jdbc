package com.j256.ormlite.jdbc.db;

/**
 * Microsoft SQL server database type information connected through the JTDS JDBC driver.
 * 
 * <p>
 * <b>NOTE:</b> Currently with 1.2.4 version of the jTDS package, I'm seeing problems with Java 1.5 because jTDS is
 * using a java.sql 1.6 class.
 * </p>
 * 
 * <p>
 * See <a href="http://jtds.sourceforge.net/" >JTDS home page</a> for more information. To use this driver, you need to
 * specify the database URL as something like the following. See the URL for more information.
 * </p>
 * 
 * <p>
 * <blockquote> jdbc:jtds:sqlserver://host-name:host-port/database-name </blockquote>
 * </p>
 * 
 * @author graywatson
 */
public class SqlServerJtdsDatabaseType extends SqlServerDatabaseType {

	private final static String DATABASE_URL_PORTION = "jtds";
	private final static String DRIVER_CLASS_NAME = "net.sourceforge.jtds.jdbc.Driver";
	private final static String DATABASE_NAME = "SQL Server JTDS";

	@Override
	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		return DATABASE_URL_PORTION.equals(dbTypePart);
	}

	@Override
	protected String[] getDriverClassNames() {
		return new String[] { DRIVER_CLASS_NAME };
	}

	@Override
	public String getDatabaseName() {
		return DATABASE_NAME;
	}
}
