package com.j256.ormlite.jdbc;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.jdbc.db.DatabaseTypeUtils;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.logger.LoggerFactory;
import com.j256.ormlite.misc.IOUtils;
import com.j256.ormlite.support.BaseConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.support.DatabaseConnectionProxyFactory;

/**
 * Base class that defines some of the common JDBC connection source functionality.
 * 
 * @author graywatson
 */
public abstract class BaseJdbcConnectionSource extends BaseConnectionSource implements ConnectionSource {

	protected static Logger logger = LoggerFactory.getLogger(BaseJdbcConnectionSource.class);

	protected String url;
	protected DatabaseConnection connection;
	protected DatabaseType databaseType;
	protected boolean initialized = false;
	private static DatabaseConnectionProxyFactory connectionProxyFactory;

	/**
	 * Constructor for Spring type wiring if you are using the set methods. If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public BaseJdbcConnectionSource() {
		// for spring type wiring
	}

	/**
	 * Create a data source for a particular database URL.
	 * 
	 * @param url
	 *            The database URL which should start jdbc:...
	 * @throws SQLException
	 *             If the driver associated with the database driver is not found in the classpath.
	 */
	public BaseJdbcConnectionSource(String url) throws SQLException {
		this(url, null, true);
	}

	/**
	 * Create a data source for a particular database URL. The databaseType is usually determined from the databaseUrl
	 * so most users should call {@link #JdbcConnectionSource(String)} instead. If, however, you need to force the class
	 * to use a specific DatabaseType then this constructor should be used.
	 * 
	 * @param url
	 *            The database URL which should start jdbc:...
	 * @param databaseType
	 *            Database to associate with this connection source.
	 * @throws SQLException
	 *             If the driver associated with the database driver is not found in the classpath.
	 */
	public BaseJdbcConnectionSource(String url, DatabaseType databaseType) throws SQLException {
		this(url, databaseType, true);
	}

	/**
	 * Set initialize to false if you don't want to initialize. This is used by subclasses.
	 */
	protected BaseJdbcConnectionSource(String url, DatabaseType databaseType, boolean initialize) throws SQLException {
		this.url = url;
		this.databaseType = databaseType;
		if (initialize) {
			initialize();
		}
	}

	/**
	 * Make a connection to the database.
	 * 
	 * @param logger
	 *            This is here so we can use the right logger associated with the sub-class.
	 */
	protected abstract DatabaseConnection makeConnection(Logger logger) throws SQLException;

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
		if (url == null) {
			throw new SQLException("url was never set on " + getClass().getSimpleName());
		}
		if (databaseType == null) {
			databaseType = DatabaseTypeUtils.createDatabaseType(url);
		}
		databaseType.loadDriver();
		databaseType.setDriver(DriverManager.getDriver(url));
		initialized = true;
	}

	@Override
	public void close() throws IOException {
		if (!initialized) {
			throw new IOException(getClass().getSimpleName() + " was not initialized properly");
		}
		if (connection != null) {
			connection.close();
			logger.debug("closed connection #{}", connection.hashCode());
			connection = null;
		}
	}

	@Override
	public void closeQuietly() {
		IOUtils.closeQuietly(this);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public DatabaseConnection getReadOnlyConnection(String tableName) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		return getReadWriteConnection(tableName);
	}

	@Override
	public DatabaseConnection getReadWriteConnection(String tableName) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		if (connection != null) {
			if (connection.isClosed()) {
				throw new SQLException("Connection has already been closed");
			} else {
				return connection;
			}
		}
		connection = makeConnection(logger);
		if (connectionProxyFactory != null) {
			connection = connectionProxyFactory.createProxy(connection);
		}
		return connection;
	}

	@Override
	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		// noop right now
	}

	@Override
	@SuppressWarnings("unused")
	public boolean saveSpecialConnection(DatabaseConnection connection) throws SQLException {
		// noop since this is a single connection source
		return true;
	}

	@Override
	public void clearSpecialConnection(DatabaseConnection connection) {
		// noop since this is a single connection source
	}

	@Override
	public DatabaseType getDatabaseType() {
		if (!initialized) {
			throw new IllegalStateException(getClass().getSimpleName() + " was not initialized properly");
		}
		return databaseType;
	}

	@Override
	public boolean isOpen(String tableName) {
		return connection != null;
	}

	@Override
	public boolean isSingleConnection(String tableName) {
		return true;
	}

	// not required
	public void setDatabaseType(DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	/**
	 * Set to enable connection proxying. Set to null to disable.
	 */
	public static void setDatabaseConnectionProxyFactory(DatabaseConnectionProxyFactory connectionProxyFactory) {
		BaseJdbcConnectionSource.connectionProxyFactory = connectionProxyFactory;
	}
}
