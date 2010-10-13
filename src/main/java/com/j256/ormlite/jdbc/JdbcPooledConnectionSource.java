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
	private boolean usesTransactions = false;
	private List<ConnectionMetaData> connList = new ArrayList<ConnectionMetaData>();
	private ThreadLocal<DatabaseConnection> transactionConnection = new ThreadLocal<DatabaseConnection>();
	private Map<DatabaseConnection, ConnectionMetaData> connectionMap =
			new HashMap<DatabaseConnection, ConnectionMetaData>();
	private final Object lock = new Object();

	private int openCount = 0;
	private int closeCount = 0;
	private int maxInUse = 0;

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
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		logger.debug("closing");
		synchronized (lock) {
			// close the outstanding connections in the list
			for (ConnectionMetaData connMetaData : connList) {
				closeConnection(connMetaData.connection);
			}
			connList.clear();
			connList = null;
			// NOTE: We can't close the ones left in the connectionMap because they may still be in use.
			connectionMap.clear();
		}
	}

	@Override
	public DatabaseConnection getReadOnlyConnection() throws SQLException {
		return getReadWriteConnection();
	}

	@Override
	public DatabaseConnection getReadWriteConnection() throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		if (usesTransactions) {
			DatabaseConnection stored = transactionConnection.get();
			if (stored != null) {
				return stored;
			}
		}
		synchronized (lock) {
			long now = System.currentTimeMillis();
			while (connList.size() > 0) {
				// take the first one off of the list
				ConnectionMetaData connMetaData = connList.remove(0);
				// is it already expired
				if (connMetaData.isExpired(now)) {
					// close expired connection
					closeConnection(connMetaData.connection);
				} else {
					logger.debug("reusing connection {}", connMetaData);
					return connMetaData.connection;
				}
			}
			// if none in the list then make a new one
			DatabaseConnection connection = makeConnection(logger);
			openCount++;
			// add it to our connection map
			connectionMap.put(connection, new ConnectionMetaData(connection));
			if (connectionMap.size() > maxInUse) {
				maxInUse = connectionMap.size();
			}
			return connection;
		}
	}

	@Override
	public void releaseConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		if (usesTransactions && transactionConnection.get() == connection) {
			// ignore the release when we are in a transaction
			return;
		}
		synchronized (lock) {
			if (connList == null) {
				// if we've already closed the pool then just close the connection
				closeConnection(connection);
				return;
			}
			ConnectionMetaData meta = connectionMap.get(connection);
			if (meta == null) {
				logger.error("should have found connection {} in the map", connection);
				closeConnection(connection);
			} else {
				connList.add(meta);
				logger.debug("cache released connection {}", meta);
				if (connList.size() > maxConnectionsFree) {
					// close the first connection in the queue
					meta = connList.remove(0);
					logger.debug("cache too full, closing connection {}", meta);
					closeConnection(meta.connection);
				}
			}
		}
	}

	@Override
	public void saveTransactionConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		/*
		 * This is fine to not be synchronized since it is only this thread we care about. Other threads will set this
		 * or have it synchronized in over time.
		 */
		usesTransactions = true;
		transactionConnection.set(connection);
		if (logger.isDebugEnabled()) {
			ConnectionMetaData meta = connectionMap.get(connection);
			logger.debug("saved trxn connection {}", meta);
		}
	}

	@Override
	public void clearTransactionConnection(DatabaseConnection connection) throws SQLException {
		if (!initialized) {
			throw new SQLException(getClass().getSimpleName() + " was not initialized properly");
		}
		transactionConnection.set(null);
		// release is then called after the clear
		if (logger.isDebugEnabled()) {
			ConnectionMetaData meta = connectionMap.get(connection);
			logger.debug("cleared trxn connection {}", meta);
		}
	}

	public void setUsesTransactions(boolean usesTransactions) {
		this.usesTransactions = usesTransactions;
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
	 * Return the number of connections opened over the life of the pool.
	 */
	public int getOpenCount() {
		return openCount;
	}

	/**
	 * Return the number of connections closed over the life of the pool.
	 */
	public int getCloseCount() {
		return closeCount;
	}

	/**
	 * Return the maximum number of connections in use at one time.
	 */
	public int getMaxConnectionsInUse() {
		return maxInUse;
	}

	/**
	 * Return the number of current connections that we are tracking.
	 */
	public int getCurrentConnectionsManaged() {
		return connectionMap.size();
	}

	private void closeConnection(DatabaseConnection connection) throws SQLException {
		ConnectionMetaData meta = connectionMap.remove(connection);
		connection.close();
		logger.debug("closed connection {}", meta);
		closeCount++;
	}

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
