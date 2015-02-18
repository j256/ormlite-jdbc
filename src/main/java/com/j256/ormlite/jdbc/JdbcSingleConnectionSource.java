package com.j256.ormlite.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;

/**
 * A connection sounds that uses an existing open database connection. This is not thread-safe nor synchronized. For
 * other dataSources, see the {@link DataSourceConnectionSource} class.
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * @author graywatson
 */
public class JdbcSingleConnectionSource extends JdbcConnectionSource implements ConnectionSource {

	private Connection sqlConnection;

	/**
	 * Constructor for Spring type wiring if you are using the set methods. If you are using Spring then your should
	 * use: init-method="initialize"
	 */
	public JdbcSingleConnectionSource() {
		// for spring type wiring
	}

	/**
	 * Create a data source for a particular database URL.
	 * 
	 * @param url
	 *            The database URL which should start jdbc:...
	 * @param sqlConnection
	 *            Already open database connection that we will use.
	 * @throws SQLException
	 *             If the driver associated with the database driver is not found in the classpath.
	 */
	public JdbcSingleConnectionSource(String url, Connection sqlConnection) throws SQLException {
		this(url, null, sqlConnection);
	}

	/**
	 * Create a data source for a particular database URL. The databaseType is usually determined from the databaseUrl
	 * so most users should call {@link #JdbcSingleConnectionSource(String, Connection)} instead. If, however, you need
	 * to force the class to use a specific DatabaseType then this constructor should be used.
	 * 
	 * @param url
	 *            The database URL which should start jdbc:...
	 * @param databaseType
	 *            Database to associate with this connection source.
	 * @param sqlConnection
	 *            Already open database connection that we will use.
	 * @throws SQLException
	 *             If the driver associated with the database driver is not found in the classpath.
	 */
	public JdbcSingleConnectionSource(String url, DatabaseType databaseType, Connection sqlConnection)
			throws SQLException {
		super(url, null, null, databaseType);
		this.sqlConnection = sqlConnection;
	}

	@Override
	public void initialize() throws SQLException {
		super.initialize();
		this.connection = new JdbcDatabaseConnection(sqlConnection);
	}

	@Override
	public void close() {
		// no-op because we don't want to close the connection
	}

	@Override
	public void releaseConnection(DatabaseConnection connection) {
		// no-op because we don't want to close the connection
	}

	// required
	public void setSqlConnection(Connection sqlConnection) {
		this.sqlConnection = sqlConnection;
	}
}
