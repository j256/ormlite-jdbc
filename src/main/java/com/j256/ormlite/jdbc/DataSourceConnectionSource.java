package com.j256.ormlite.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DatabaseTypeUtils;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.logger.LoggerFactory;
import com.j256.ormlite.support.BaseConnectionSource;
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
public class DataSourceConnectionSource extends BaseConnectionSource implements ConnectionSource {

	private static Logger logger = LoggerFactory.getLogger(DataSourceConnectionSource.class);

	private DataSource dataSource;
	private DatabaseType databaseType;
	private String databaseUrl;
	private boolean initialized = false;

	/**
	 * Constructor for Spring type wiring if you are using the set methods. If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public DataSourceConnectionSource() {
		// for spring type wiring
	}

	/**
	 * Create a data source wrapper for a DataSource.
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
	 * Create a data source wrapper for a DataSource. The databaseType is usually determined from the databaseUrl so
	 * most users should call {@link #DataSourceConnectionSource(DataSource, String)} instead. If, however, you need to
	 * force the class to use a specific DatabaseType then this constructor should be used.
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
	 * Initialize the class after the setters have been called. If you are using the no-arg constructor and Spring type
	 * wiring, this should be called after all of the set methods.
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
		if (databaseUrl != null) {
			databaseType.setDriver(DriverManager.getDriver(databaseUrl));
		}
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
		DatabaseConnection saved = getSavedConnection();
		if (saved != null) {
			return saved;
		}
		return new JdbcDatabaseConnection(dataSource.getConnection());
	}

	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		if (isSavedConnection(connection)) {
			// ignore the release because we will close it at the end of the connection
		} else {
			connection.close();
		}
	}

	public DatabaseConnection getReadWriteConnection(String username, String password) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		DatabaseConnection saved = getSavedConnection();
		if (saved != null) {
			return saved;
		}
		return new JdbcDatabaseConnection(dataSource.getConnection(username, password));
	}

	public boolean saveSpecialConnection(DatabaseConnection connection) throws SQLException {
		/*
		 * This is fine to not be synchronized since it is only this thread we care about. Other threads will set this
		 * or have it synchronized in over time.
		 */
		return saveSpecial(connection);
	}

	public void clearSpecialConnection(DatabaseConnection connection) {
		clearSpecial(connection, logger);
	}

	/**
	 * This typically closes the connection source but because there is not a close() method on the {@link DataSource}
	 * (grrrr), this close method does _nothing_. You must close the underlying data-source yourself.
	 */
	public void close() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + ".initialize() was not called");
		}
		// unfortunately, you will need to close the DataSource directly since there is no close on the interface
	}

	public void closeQuietly() {
		try {
			close();
		} catch (SQLException e) {
			// ignored
		}
	}

	public DatabaseType getDatabaseType() {
		if (!initialized) {
			throw new IllegalStateException(getClass().getSimpleName() + ".initialize() was not called");
		}
		return databaseType;
	}

	/**
	 * Unfortunately we cannot tell if the related data source has been closed so this just returns true.
	 */
	public boolean isOpen() {
		return true;
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

	/**
	 * @deprecated No longer supported and can be removed.
	 */
	@Deprecated
	public void setUsesTransactions(boolean usesTransactions) {
	}
}
