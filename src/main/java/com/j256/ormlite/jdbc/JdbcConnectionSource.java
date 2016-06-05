package com.j256.ormlite.jdbc;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DatabaseTypeUtils;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.logger.LoggerFactory;
import com.j256.ormlite.misc.IOUtils;
import com.j256.ormlite.support.BaseConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.support.DatabaseConnectionProxyFactory;

/**
 * Implementation of the ConnectionSource interface that supports what is needed by ORMLite. This is not thread-safe nor
 * synchronized and under the covers uses a single database connection. For other dataSources, see the
 * {@link DataSourceConnectionSource} class.
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * @author graywatson
 */
public class JdbcConnectionSource extends BaseConnectionSource implements ConnectionSource {

	private static Logger logger = LoggerFactory.getLogger(JdbcConnectionSource.class);

	private String url;
	private String username;
	private String password;
	protected DatabaseConnection connection;
	protected DatabaseType databaseType;
	protected boolean initialized = false;
	private static DatabaseConnectionProxyFactory connectionProxyFactory;

	/**
	 * Constructor for Spring type wiring if you are using the set methods. If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public JdbcConnectionSource() {
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
	public JdbcConnectionSource(String url) throws SQLException {
		this(url, null, null, null);
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
	public JdbcConnectionSource(String url, DatabaseType databaseType) throws SQLException {
		this(url, null, null, databaseType);
	}

	/**
	 * Create a data source for a particular database URL with username and password permissions.
	 * 
	 * @param url
	 *            The database URL which should start jdbc:...
	 * @param username
	 *            Username for permissions on the database.
	 * @param password
	 *            Password for permissions on the database.
	 * @throws SQLException
	 *             If the driver associated with the database driver is not found in the classpath.
	 */
	public JdbcConnectionSource(String url, String username, String password) throws SQLException {
		this(url, username, password, null);
	}

	/**
	 * Create a data source for a particular database URL with username and password permissions. The databaseType is
	 * usually determined from the databaseUrl so most users should call
	 * {@link #JdbcConnectionSource(String, String, String)} instead. If, however, you need to force the class to use a
	 * specific DatabaseType then this constructor should be used.
	 * 
	 * @param url
	 *            The database URL which should start jdbc:...
	 * @param username
	 *            Username for permissions on the database.
	 * @param password
	 *            Password for permissions on the database.
	 * @param databaseType
	 *            Database to associate with this connection source.
	 * @throws SQLException
	 *             If the driver associated with the database driver is not found in the classpath.
	 */
	public JdbcConnectionSource(String url, String username, String password, DatabaseType databaseType)
			throws SQLException {
		this.url = url;
		this.username = username;
		this.password = password;
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
	public void setUsername(String username) {
		this.username = username;
	}

	// not required
	public void setPassword(String password) {
		this.password = password;
	}

	// not required
	public void setDatabaseType(DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	/**
	 * Set to enable connection proxying. Set to null to disable.
	 */
	public static void setDatabaseConnectionProxyFactory(DatabaseConnectionProxyFactory connectionProxyFactory) {
		JdbcConnectionSource.connectionProxyFactory = connectionProxyFactory;
	}

	/**
	 * Make a connection to the database.
	 * 
	 * @param logger
	 *            This is here so we can use the right logger associated with the sub-class.
	 */
	@SuppressWarnings("resource")
	protected DatabaseConnection makeConnection(Logger logger) throws SQLException {
		Properties properties = new Properties();
		if (username != null) {
			properties.setProperty("user", username);
		}
		if (password != null) {
			properties.setProperty("password", password);
		}
		DatabaseConnection connection = new JdbcDatabaseConnection(DriverManager.getConnection(url, properties));
		// by default auto-commit is set to true
		connection.setAutoCommit(true);
		if (connectionProxyFactory != null) {
			connection = connectionProxyFactory.createProxy(connection);
		}
		logger.debug("opened connection to {} got #{}", url, connection.hashCode());
		return connection;
	}
}
