package com.j256.ormlite.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DatabaseTypeUtils;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.logger.LoggerFactory;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;

/**
 * Implementation of the ConnectionSource interface that supports what is needed by ORMLite. This is not thread-safe nor
 * synchronized. For other dataSources, see the {@link DataSourceConnectionSource} class.
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * @author graywatson
 */
public class JdbcConnectionSource implements ConnectionSource {

	private static Logger logger = LoggerFactory.getLogger(JdbcConnectionSource.class);

	private String url;
	private String username;
	private String password;
	private JdbcDatabaseConnection connection;
	private DatabaseType databaseType;
	private boolean initialized = false;

	/**
	 * Constructor for Spring type wiring if you are using the set methods.
	 */
	public JdbcConnectionSource() {
		// for spring wiring
	}

	/**
	 * Create a data source for a particular database URL.
	 * 
	 * @throws SQLException
	 *             If the driver associated with the database URL is not found in the classpath.
	 */
	public JdbcConnectionSource(String url) throws SQLException {
		this(url, null, null);
	}

	/**
	 * Create a data source for a particular database URL with username and password permissions.
	 * 
	 * @throws SQLException
	 *             If the driver associated with the database URL is not found in the classpath.
	 */
	public JdbcConnectionSource(String url, String username, String password) throws SQLException {
		this.url = url;
		this.username = username;
		this.password = password;
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
		if (url == null) {
			throw new SQLException("url was never set on " + getClass().getSimpleName());
		}
		if (databaseType == null) {
			databaseType = DatabaseTypeUtils.createDatabaseType(url);
		}
		databaseType.loadDriver();
		initialized = true;
	}

	public void close() throws SQLException {
		if (connection != null) {
			connection.close();
			connection = null;
			logger.debug("closed connection to {}", url);
		}
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public DatabaseConnection getReadOnlyConnection() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		return getReadWriteConnection();
	}

	public DatabaseConnection getReadWriteConnection() throws SQLException {
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
		Properties properties = new Properties();
		if (username != null) {
			properties.setProperty("user", username);
		}
		if (password != null) {
			properties.setProperty("password", password);
		}
		logger.debug("opening connection to {}", url);
		connection = new JdbcDatabaseConnection(DriverManager.getConnection(url, properties));
		if (connection == null) {
			// may never get here but let's be careful
			throw new SQLException("Could not establish connection to database URL: " + url);
		} else {
			return connection;
		}
	}

	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		// noop right now
	}

	public DatabaseType getDatabaseType() {
		if (!initialized) {
			throw new IllegalStateException(getClass().getSimpleName() + " was not initialized properly");
		}
		return databaseType;
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
}