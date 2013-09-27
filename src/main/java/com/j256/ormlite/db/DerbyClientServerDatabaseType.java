package com.j256.ormlite.db;

/**
 * Derby database type information used to create the tables, etc.. This is for client connections to a remote Derby
 * server. For embedded databases, you should use {@link DerbyEmbeddedDatabaseType}.
 * 
 * @author graywatson
 */
public class DerbyClientServerDatabaseType extends DerbyEmbeddedDatabaseType {

	private final static String DRIVER_CLASS_NAME = "org.apache.derby.jdbc.ClientDriver";
	private final static String DATABASE_NAME = "Derby Client/Server";

	@Override
	public boolean isDatabaseUrlThisType(String url, String dbTypePart) {
		if (!DATABASE_URL_PORTION.equals(dbTypePart)) {
			return false;
		}
		// jdbc:derby://localhost:1527/MyDbTest;create=true';
		String[] parts = url.split(":");
		return (parts.length >= 3 && parts[2].startsWith("//"));
	}

	@Override
	public String getDriverClassName() {
		return DRIVER_CLASS_NAME;
	}

	@Override
	public String getDatabaseName() {
		return DATABASE_NAME;
	}
}
