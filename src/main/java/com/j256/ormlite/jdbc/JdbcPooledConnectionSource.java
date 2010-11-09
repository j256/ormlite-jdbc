package com.j256.ormlite.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.logger.LoggerFactory;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;

/**
 * Implementation of the ConnectionSource interface that supports basic pooled connections. New connections are created
 * on demand only if there are no dormant connections otherwise released connections will be reused. This class is
 * reentrant and can handle requests from multiple threads.
 * 
 * <p>
 * <b> WARNING: </b> As of 10/2010 this is one of the newer parts of ORMLite meaning it may still have bugs. Additional
 * review of the code and any feedback would be appreciated.
 * </p>
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * @author graywatson
 */
public class JdbcPooledConnectionSource extends JdbcConnectionSource implements ConnectionSource {

	private static Logger logger = LoggerFactory.getLogger(JdbcPooledConnectionSource.class);
	private final static int DEFAULT_MAX_CONNECTIONS_FREE = 5;
	// maximum age that a connection can be before being closed
	private final static int DEFAULT_MAX_CONNECTION_AGE_MILLIS = 60 * 60 * 1000;

	private int maxConnectionsFree = DEFAULT_MAX_CONNECTIONS_FREE;
	private long maxConnectionAgeMillis = DEFAULT_MAX_CONNECTION_AGE_MILLIS;
	private List<ConnectionMetaData> connFreeList = new ArrayList<ConnectionMetaData>();
	private Map<DatabaseConnection, ConnectionMetaData> connectionMap =
			new HashMap<DatabaseConnection, ConnectionMetaData>();
	private final Object lock = new Object();

	private int openCount = 0;
	private int closeCount = 0;
	private int maxEverUsed = 0;

	public JdbcPooledConnectionSource() {
		// for spring type wiring
	}

	public JdbcPooledConnectionSource(String url) throws SQLException {
		super(url, null, null, null);
	}

	public JdbcPooledConnectionSource(String url, DatabaseType databaseType) throws SQLException {
		super(url, null, null, databaseType);
	}

	public JdbcPooledConnectionSource(String url, String username, String password) throws SQLException {
		super(url, username, password, null);
	}

	public JdbcPooledConnectionSource(String url, String username, String password, DatabaseType databaseType)
			throws SQLException {
		super(url, username, password, databaseType);
	}

	@Override
	public void close() throws SQLException {
		checkInitializedSqlException();
		logger.debug("closing");
		synchronized (lock) {
			// close the outstanding connections in the list
			for (ConnectionMetaData connMetaData : connFreeList) {
				closeConnection(connMetaData.connection);
			}
			connFreeList.clear();
			connFreeList = null;
			// NOTE: We can't close the ones left in the connectionMap because they may still be in use.
			connectionMap.clear();
		}
	}

	@Override
	public DatabaseConnection getReadOnlyConnection() throws SQLException {
		// set the connection to be read-only in JDBC-land? would need to set read-only or read-write
		return getReadWriteConnection();
	}

	@Override
	public DatabaseConnection getReadWriteConnection() throws SQLException {
		checkInitializedSqlException();
		DatabaseConnection conn = getSavedConnection();
		if (conn != null) {
			return conn;
		}
		synchronized (lock) {
			long now = System.currentTimeMillis();
			while (connFreeList.size() > 0) {
				// take the first one off of the list
				ConnectionMetaData connMetaData = connFreeList.remove(0);
				// is it already expired
				if (connMetaData.isExpired(now)) {
					// close expired connection
					closeConnection(connMetaData.connection);
				} else {
					logger.debug("reusing connection {}", connMetaData);
					return connMetaData.connection;
				}
			}
			// if none in the free list then make a new one
			DatabaseConnection connection = makeConnection(logger);
			openCount++;
			// add it to our connection map
			connectionMap.put(connection, new ConnectionMetaData(connection));
			int maxInUse = connectionMap.size();
			if (maxInUse > maxEverUsed) {
				maxEverUsed = maxInUse;
			}
			return connection;
		}
	}

	@Override
	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		checkInitializedSqlException();
		if (isSavedConnection(connection)) {
			// ignore the release when we are in a transaction
			return;
		}
		synchronized (lock) {
			if (connection.isClosed()) {
				// it's already closed so just drop it
				ConnectionMetaData meta = connectionMap.remove(connection);
				if (meta == null) {
					logger.debug("dropping already closed unknown connection {}", connection);
				} else {
					logger.debug("dropping already closed connection {}", meta);
				}
				return;
			}
			if (connFreeList == null) {
				// if we've already closed the pool then just close the connection
				closeConnection(connection);
				return;
			}
			ConnectionMetaData meta = connectionMap.get(connection);
			if (meta == null) {
				logger.error("should have found connection {} in the map", connection);
				closeConnection(connection);
			} else {
				connFreeList.add(meta);
				logger.debug("cache released connection {}", meta);
				if (connFreeList.size() > maxConnectionsFree) {
					// close the first connection in the queue
					meta = connFreeList.remove(0);
					logger.debug("cache too full, closing connection {}", meta);
					closeConnection(meta.connection);
				}
			}
		}
	}

	@Override
	public boolean saveSpecialConnection(DatabaseConnection connection) {
		checkInitializedIllegalStateException();
		boolean saved = saveSpecial(connection);
		if (logger.isDebugEnabled()) {
			ConnectionMetaData meta = connectionMap.get(connection);
			logger.debug("saved special connection {}", meta);
		}
		return saved;
	}

	@Override
	public void clearSpecialConnection(DatabaseConnection connection) {
		checkInitializedIllegalStateException();
		clearSpecial(connection, logger);
		if (logger.isDebugEnabled()) {
			ConnectionMetaData meta = connectionMap.get(connection);
			logger.debug("cleared special connection {}", meta);
		}
		// release should then called after the clear
	}

	public void setUsesTransactions(boolean usesTransactions) {
		this.usedSpecialConnection = usesTransactions;
	}

	/**
	 * Set the number of connections that can be unused in the available list.
	 */
	public void setMaxConnectionsFree(int maxConnectionsFree) {
		this.maxConnectionsFree = maxConnectionsFree;
	}

	/**
	 * Set the number of milliseconds that a connection can stay open before being closed. Set to Long.MAX_VALUE to have
	 * the connections never expire.
	 */
	public void setMaxConnectionAgeMillis(long maxConnectionAgeMillis) {
		this.maxConnectionAgeMillis = maxConnectionAgeMillis;
	}

	/**
	 * Return the approximate number of connections opened over the life of the pool.
	 */
	public int getOpenCount() {
		return openCount;
	}

	/**
	 * Return the approximate number of connections closed over the life of the pool.
	 */
	public int getCloseCount() {
		return closeCount;
	}

	/**
	 * Return the approximate maximum number of connections in use at one time.
	 */
	public int getMaxConnectionsEverUsed() {
		return maxEverUsed;
	}

	/**
	 * Return the number of current connections that we are tracking.
	 */
	public int getCurrentConnectionsManaged() {
		synchronized (lock) {
			return connectionMap.size();
		}
	}

	private void checkInitializedSqlException() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
	}

	private void checkInitializedIllegalStateException() {
		if (!initialized) {
			throw new IllegalStateException(getClass().getSimpleName() + " was not initialized properly");
		}
	}

	/**
	 * This should be inside of synchronized (lock) stanza.
	 */
	private void closeConnection(DatabaseConnection connection) throws SQLException {
		// this can return null if we are closing the pool
		ConnectionMetaData meta = connectionMap.remove(connection);
		connection.close();
		logger.debug("closed connection {}", meta);
		closeCount++;
	}

	/**
	 * Class to hold the connection and its meta data.
	 */
	private class ConnectionMetaData {
		public final DatabaseConnection connection;
		private final long expiresMillis;

		public ConnectionMetaData(DatabaseConnection connection) {
			this.connection = connection;
			long now = System.currentTimeMillis();
			if (maxConnectionAgeMillis > Long.MAX_VALUE - now) {
				this.expiresMillis = Long.MAX_VALUE;
			} else {
				this.expiresMillis = now + maxConnectionAgeMillis;
			}
		}

		public boolean isExpired(long now) {
			return (expiresMillis <= now);
		}

		@Override
		public String toString() {
			return "@" + Integer.toHexString(hashCode());
		}
	}
}
