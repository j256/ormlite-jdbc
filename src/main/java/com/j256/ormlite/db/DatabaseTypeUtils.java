package com.j256.ormlite.db;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.j256.ormlite.jdbc.JdbcConnectionSource;

/**
 * Utility class which helps with managing database specific classes.
 * 
 * @author graywatson
 */
public class DatabaseTypeUtils {

	private static Map<String, Constructor<? extends DatabaseType>> constructorMap =
			new HashMap<String, Constructor<? extends DatabaseType>>();

	static {
		// new drivers need to be added here
		addDriver(MysqlDatabaseType.class);
		addDriver(PostgresDatabaseType.class);
		addDriver(H2DatabaseType.class);
		addDriver(DerbyEmbeddedDatabaseType.class);
		addDriver(SqliteDatabaseType.class);
		addDriver(HsqldbDatabaseType.class);
		addDriver(OracleDatabaseType.class);
		addDriver(SqlServerDatabaseType.class);
		addDriver(SqlServerJtdsDatabaseType.class);
		addDriver(Db2DatabaseType.class);
	}

	/**
	 * For static methods only.
	 */
	private DatabaseTypeUtils() {
	}

	/**
	 * @deprecated The {@link JdbcConnectionSource} does this automatically now.
	 */
	@Deprecated
	public static void loadDriver(String databaseUrl) throws ClassNotFoundException {
		// does nothing since the JdbcConnectionSource does it now
	}

	/**
	 * @deprecated Use {@link JdbcConnectionSource#JdbcConnectionSource(String)}
	 */
	@Deprecated
	public static JdbcConnectionSource createJdbcConnectionSource(String databaseUrl) throws SQLException {
		return new JdbcConnectionSource(databaseUrl);
	}

	/**
	 * @deprecated Use {@link JdbcConnectionSource#JdbcConnectionSource(String, String, String)}
	 */
	@Deprecated
	public static JdbcConnectionSource createJdbcConnectionSource(String databaseUrl, String userName, String password)
			throws SQLException {
		return new JdbcConnectionSource(databaseUrl, userName, password);
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
		Constructor<? extends DatabaseType> constructor = constructorMap.get(dbTypePart);
		if (constructor == null) {
			throw new IllegalArgumentException("Unknown database-type url part '" + dbTypePart + "' in: " + databaseUrl);
		}
		try {
			return constructor.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Problems calling constructor " + constructor, e);
		}
	}

	private static void addDriver(Class<? extends DatabaseType> dbClass) {
		DatabaseType driverType;
		Constructor<? extends DatabaseType> constructor;
		try {
			constructor = dbClass.getConstructor();
			driverType = constructor.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException("Could not construct driver class " + dbClass, e);
		}
		String urlPart = driverType.getDriverUrlPart();
		if (!constructorMap.containsKey(urlPart)) {
			constructorMap.put(urlPart, constructor);
		}
	}

	private static String extractDbType(String databaseUrl) {
		if (!databaseUrl.startsWith("jdbc:")) {
			throw new IllegalArgumentException("Database URL was expected to start with jdbc: but was " + databaseUrl);
		}
		String[] urlParts = databaseUrl.split(":");
		if (urlParts.length < 2) {
			throw new IllegalArgumentException("Database URL was expected to be in the form: jdbc:db-type:... but was "
					+ databaseUrl);
		}
		return urlParts[1];
	}
}
