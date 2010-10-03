package com.j256.ormlite.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DatabaseTypeUtils;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;

/**
 * Wrapper around a {@link DataSource} that supports our ConnectionSource interface. This allows you to wrap other
 * multi-threaded, high-performance data sources, see Apache DBCP, CP30, or BoneCP.
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * @author graywatson
 */
public class DataSourceConnectionSource implements ConnectionSource {

	private DataSource dataSource;
	private DatabaseType databaseType;
	private String databaseUrl;
	private boolean initialized = false;

	/**
	 * Constructor for Spring type wiring if you are using the set methods.
	 */
	public DataSourceConnectionSource() {
		// for spring wiring
	}

	/**
	 * Create a data source for a particular database URL.
	 * 
	 * @throws SQLException
	 *             If the driver associated with the database URL is not found in the classpath.
	 */
	public DataSourceConnectionSource(DataSource dataSource, String databaseUrl) throws SQLException {
		this.dataSource = dataSource;
		this.databaseUrl = databaseUrl;
		initialize();
	}

	/**
	 * Create a data source for a particular database URL. The databaseType is usually determined from the databaseUrl
	 * so most users should call {@link #DataSourceConnectionSource(DataSource, String)} instead. If, however, you need
	 * to force the class to use a specific DatabaseType then this constructor should be used.
	 * 
	 * @throws SQLException
	 *             If the driver associated with the database URL is not found in the classpath.
	 */
	public DataSourceConnectionSource(DataSource dataSource, DatabaseType databaseType) throws SQLException {
		this.dataSource = dataSource;
		this.databaseType = databaseType;
		initialize();
	}

	/**
	 * If you are using the Spring type wiring, this should be called after all of the set methods.
	 * 
	 * @throws SQLException
	 *             If the driver associated with the database URL is not found in the classpath.
	 */
	public void initialize() throws SQLException {
		if (initialized) {
			return;
		}
		if (dataSource == null) {
			throw new IllegalStateException("dataSource was never set on " + getClass().getSimpleName());
		}
		if (databaseType == null) {
			if (databaseUrl == null) {
				throw new IllegalStateException("either the databaseUri or the databaseType must be set on "
						+ getClass().getSimpleName());
			}
			databaseType = DatabaseTypeUtils.createDatabaseType(databaseUrl);
		}
		databaseType.loadDriver();
		initialized = true;
	}

	public DatabaseConnection getReadOnlyConnection() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		return getReadWriteConnection();
	}

	public DatabaseConnection getReadOnlyConnection(String username, String password) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		return getReadWriteConnection(username, password);
	}

	public DatabaseConnection getReadWriteConnection() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		return new JdbcDatabaseConnection(dataSource.getConnection());
	}

	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		// noop right now
	}

	public DatabaseConnection getReadWriteConnection(String username, String password) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		return new JdbcDatabaseConnection(dataSource.getConnection(username, password));
	}

	public void close() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		// unfortunately, you will need to close the DataSource directly since there is no close on the interface
	}

	public DatabaseType getDatabaseType() {
		if (!initialized) {
			throw new IllegalStateException(getClass().getSimpleName() + ".initialize() was not called");
		}
		return databaseType;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void setDatabaseType(DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	public void setDatabaseUrl(String databaseUrl) {
		this.databaseUrl = databaseUrl;
	}
}
