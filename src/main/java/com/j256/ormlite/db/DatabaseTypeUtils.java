package com.j256.ormlite.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class which helps with managing database specific classes.
 * 
 * @author graywatson
 */
public class DatabaseTypeUtils {

	private static List<DatabaseType> databaseTypes = new ArrayList<DatabaseType>();

	static {
		// new drivers need to be added here
		databaseTypes.add(new Db2DatabaseType());
		databaseTypes.add(new DerbyClientServerDatabaseType());
		databaseTypes.add(new DerbyEmbeddedDatabaseType());
		databaseTypes.add(new GenericOdbcDatabaseType());
		databaseTypes.add(new H2DatabaseType());
		databaseTypes.add(new HsqldbDatabaseType());
		databaseTypes.add(new MysqlDatabaseType());
		databaseTypes.add(new MariaDbDatabaseType());
		databaseTypes.add(new NetezzaDatabaseType());
		databaseTypes.add(new OracleDatabaseType());
		databaseTypes.add(new PostgresDatabaseType());
		databaseTypes.add(new SqliteDatabaseType());
		databaseTypes.add(new SqlServerDatabaseType());
		databaseTypes.add(new SqlServerJtdsDatabaseType());
	}

	/**
	 * For static methods only.
	 */
	private DatabaseTypeUtils() {
	}

	/**
	 * Creates and returns a {@link DatabaseType} for the database URL.
	 * 
	 * @throws IllegalArgumentException
	 *             if the url format is not recognized, the database type is unknown, or the class could not be
	 *             constructed.
	 */
	public static DatabaseType createDatabaseType(String databaseUrl) {
		String dbTypePart = extractDbType(databaseUrl);
		for (DatabaseType databaseType : databaseTypes) {
			if (databaseType.isDatabaseUrlThisType(databaseUrl, dbTypePart)) {
				return databaseType;
			}
		}
		throw new IllegalArgumentException("Unknown database-type url part '" + dbTypePart + "' in: " + databaseUrl);
	}

	private static String extractDbType(String databaseUrl) {
		if (!databaseUrl.startsWith("jdbc:")) {
			throw new IllegalArgumentException("Database URL was expected to start with jdbc: but was " + databaseUrl);
		}
		String[] urlParts = databaseUrl.split(":");
		if (urlParts.length < 2) {
			throw new IllegalArgumentException(
					"Database URL was expected to be in the form: jdbc:db-type:... but was " + databaseUrl);
		}
		return urlParts[1];
	}
}
