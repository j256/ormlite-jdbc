package com.j256.ormlite.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;

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
public class JdbcConnectionSource extends BaseJdbcConnectionSource implements ConnectionSource {

	private String username;
	private String password;

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
		this(url, null, null, null, true);
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
		this(url, null, null, databaseType, true);
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
		this(url, username, password, null, true);
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
		this(url, username, password, databaseType, true);
	}

	/**
	 * Set initialize to false if you don't want to initialize. This is used by subclasses.
	 */
	protected JdbcConnectionSource(String url, String username, String password, DatabaseType databaseType,
			boolean initialize) throws SQLException {
		super(url, databaseType);
		this.username = username;
		this.password = password;
		if (initialize) {
			initialize();
		}
	}

	// not required
	public void setUsername(String username) {
		this.username = username;
	}

	// not required
	public void setPassword(String password) {
		this.password = password;
	}

	@Override
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
		logger.debug("opened connection to {} got #{}", url, connection.hashCode());
		return connection;
	}
}
