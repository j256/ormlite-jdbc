package com.j256.ormlite.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.logger.Log.Level;
import com.j256.ormlite.logger.Logger;
import com.j256.ormlite.logger.LoggerFactory;
import com.j256.ormlite.misc.IOUtils;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;

/**
 * Implementation of the ConnectionSource interface that supports basic pooled connections. New connections are created
 * on demand only if there are no dormant connections otherwise released connections will be reused. This class is
 * reentrant and can handle requests from multiple threads.
 * 
 * <p>
 * <b> NOTE: </b> If you are using the Spring type wiring in Java, {@link #initialize} should be called after all of the
 * set methods. In Spring XML, init-method="initialize" should be used.
 * </p>
 * 
 * <p>
 * <b> NOTE: </b> This class spawns a thread to test the pooled connections that are in the free-list as a keep-alive
 * mechanism. It will test any dormant connections every so often to see if they are still valid. If this is not the
 * behavior that you want then call {@link #setCheckConnectionsEveryMillis(long)} with 0 to disable the thread. You can
 * also call {@link #setTestBeforeGet(boolean)} and set it to true to test the connection before it is handed back to
 * you.
 * </p>
 * 
 * @author graywatson
 */
public class JdbcPooledConnectionSource extends JdbcConnectionSource implements ConnectionSource {

	private static Logger logger = LoggerFactory.getLogger(JdbcPooledConnectionSource.class);
	private final static int DEFAULT_MAX_CONNECTIONS_FREE = 5;
	// maximum age that a connection can be before being closed
	private final static int DEFAULT_MAX_CONNECTION_AGE_MILLIS = 60 * 60 * 1000;
	private final static int CHECK_CONNECTIONS_EVERY_MILLIS = 30 * 1000;

	private int maxConnectionsFree = DEFAULT_MAX_CONNECTIONS_FREE;
	private long maxConnectionAgeMillis = DEFAULT_MAX_CONNECTION_AGE_MILLIS;
	private List<ConnectionMetaData> connFreeList = new ArrayList<ConnectionMetaData>();
	protected final Map<DatabaseConnection, ConnectionMetaData> connectionMap =
			new HashMap<DatabaseConnection, ConnectionMetaData>();
	private final Object lock = new Object();
	private ConnectionTester tester = null;
	private String pingStatment;

	private int openCount = 0;
	private int releaseCount = 0;
	private int closeCount = 0;
	private int maxEverUsed = 0;
	private int testLoopCount = 0;
	private long checkConnectionsEveryMillis = CHECK_CONNECTIONS_EVERY_MILLIS;
	private boolean testBeforeGetFromPool = false;
	private volatile boolean isOpen = true;

	public JdbcPooledConnectionSource() {
		// for spring type wiring
	}

	public JdbcPooledConnectionSource(String url) throws SQLException {
		this(url, null, null, null);
	}

	public JdbcPooledConnectionSource(String url, DatabaseType databaseType) throws SQLException {
		this(url, null, null, databaseType);
	}

	public JdbcPooledConnectionSource(String url, String username, String password) throws SQLException {
		this(url, username, password, null);
	}

	public JdbcPooledConnectionSource(String url, String username, String password, DatabaseType databaseType)
			throws SQLException {
		super(url, username, password, databaseType);
	}

	@Override
	public void initialize() throws SQLException {
		super.initialize();
		pingStatment = databaseType.getPingStatement();
	}

	@Override
	public void close() throws IOException {
		if (!initialized) {
			throw new IOException(getClass().getSimpleName() + " was not initialized properly");
		}
		logger.debug("closing");
		synchronized (lock) {
			// close the outstanding connections in the list
			for (ConnectionMetaData connMetaData : connFreeList) {
				closeConnectionQuietly(connMetaData);
			}
			connFreeList.clear();
			connFreeList = null;
			// NOTE: We can't close the ones left in the connectionMap because they may still be in use.
			connectionMap.clear();
			isOpen = false;
		}
	}

	@Override
	public DatabaseConnection getReadOnlyConnection(String tableName) throws SQLException {
		// set the connection to be read-only in JDBC-land? would need to set read-only or read-write
		return getReadWriteConnection(tableName);
	}

	@Override
	public DatabaseConnection getReadWriteConnection(String tableName) throws SQLException {
		checkInitializedSqlException();
		DatabaseConnection conn = getSavedConnection();
		if (conn != null) {
			return conn;
		}
		synchronized (lock) {
			while (connFreeList.size() > 0) {
				// take the first one off of the list
				ConnectionMetaData connMetaData = getFreeConnection();
				if (connMetaData == null) {
					// need to create a new one
				} else if (testBeforeGetFromPool && !testConnection(connMetaData)) {
					// close expired connection
					closeConnectionQuietly(connMetaData);
				} else {
					logger.debug("reusing connection {}", connMetaData);
					return connMetaData.connection;
				}
			}
			// if none in the free list then make a new one
			DatabaseConnection connection = makeConnection(logger);
			openCount++;
			// add it to our connection map
			connectionMap.put(connection, new ConnectionMetaData(connection, maxConnectionAgeMillis));
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
		/*
		 * If the connection is not close and has auto-commit turned off then we must roll-back any outstanding
		 * statements and set auto-commit back to true.
		 */
		boolean isClosed = connection.isClosed();
		if (!isClosed && !connection.isAutoCommit()) {
			connection.rollback(null);
			connection.setAutoCommit(true);
		}
		synchronized (lock) {
			releaseCount++;
			if (isClosed) {
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
				meta.noteUsed();
				connFreeList.add(meta);
				logger.debug("cache released connection {}", meta);
				if (connFreeList.size() > maxConnectionsFree) {
					// close the first connection in the queue
					meta = connFreeList.remove(0);
					logger.debug("cache too full, closing connection {}", meta);
					closeConnection(meta.connection);
				}
				if (checkConnectionsEveryMillis > 0 && tester == null) {
					tester = new ConnectionTester();
					tester.setName(getClass().getSimpleName() + " connection tester");
					tester.setDaemon(true);
					tester.start();
				}
			}
		}
	}

	@Override
	public boolean saveSpecialConnection(DatabaseConnection connection) throws SQLException {
		checkInitializedIllegalStateException();
		boolean saved = saveSpecial(connection);
		if (logger.isLevelEnabled(Level.DEBUG)) {
			ConnectionMetaData meta = connectionMap.get(connection);
			logger.debug("saved special connection {}", meta);
		}
		return saved;
	}

	@Override
	public void clearSpecialConnection(DatabaseConnection connection) {
		checkInitializedIllegalStateException();
		boolean cleared = clearSpecial(connection, logger);
		if (logger.isLevelEnabled(Level.DEBUG)) {
			ConnectionMetaData meta = connectionMap.get(connection);
			if (cleared) {
				logger.debug("cleared special connection {}", meta);
			} else {
				logger.debug("special connection {} not saved", meta);
			}
		}
		// release should then called after the clear
	}

	@Override
	public boolean isOpen(String tableName) {
		return isOpen;
	}

	@Override
	public boolean isSingleConnection(String tableName) {
		return false;
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
	 * Return the approximate number of connections released over the life of the pool.
	 */
	public int getReleaseCount() {
		return releaseCount;
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
	 * Return the number of currently freed connections in the free list.
	 */
	public int getCurrentConnectionsFree() {
		synchronized (lock) {
			return connFreeList.size();
		}
	}

	/**
	 * Return the number of current connections that we are tracking.
	 */
	public int getCurrentConnectionsManaged() {
		synchronized (lock) {
			return connectionMap.size();
		}
	}

	/**
	 * There is an internal thread which checks each of the database connections as a keep-alive mechanism. This set the
	 * number of milliseconds it sleeps between checks -- default is 30000. To disable the checking thread, set this to
	 * 0 before you start using the connection source.
	 */
	public void setCheckConnectionsEveryMillis(long checkConnectionsEveryMillis) {
		this.checkConnectionsEveryMillis = checkConnectionsEveryMillis;
	}

	public void setTestBeforeGet(boolean testBeforeGetFromPool) {
		this.testBeforeGetFromPool = testBeforeGetFromPool;
	}

	/**
	 * Mostly for testing purposes to see how many times our test loop ran.
	 */
	public int getTestLoopCount() {
		return testLoopCount;
	}

	/**
	 * This should be inside of synchronized (lock) stanza.
	 */
	protected void closeConnection(DatabaseConnection connection) throws SQLException {
		// this can return null if we are closing the pool
		ConnectionMetaData meta = connectionMap.remove(connection);
		IOUtils.closeThrowSqlException(connection, "SQL connection");
		logger.debug("closed connection {}", meta);
		closeCount++;
	}

	/**
	 * Must be called inside of synchronized(lock)
	 */
	protected void closeConnectionQuietly(ConnectionMetaData connMetaData) {
		try {
			// close expired connection
			closeConnection(connMetaData.connection);
		} catch (SQLException e) {
			// we ignore this
		}
	}

	protected boolean testConnection(ConnectionMetaData connMetaData) {
		try {
			// issue our ping statement
			long result = connMetaData.connection.queryForLong(pingStatment);
			logger.trace("tested connection {}, got {}", connMetaData, result);
			return true;
		} catch (Exception e) {
			logger.debug(e, "testing connection {} threw exception", connMetaData);
			return false;
		}
	}

	private ConnectionMetaData getFreeConnection() {
		synchronized (lock) {
			long now = System.currentTimeMillis();
			while (connFreeList.size() > 0) {
				// take the first one off of the list
				ConnectionMetaData connMetaData = connFreeList.remove(0);
				// is it already expired
				if (connMetaData.isExpired(now)) {
					// close expired connection
					closeConnectionQuietly(connMetaData);
				} else {
					connMetaData.noteUsed();
					return connMetaData;
				}
			}
		}
		return null;
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
	 * Class to hold the connection and its meta data.
	 */
	protected static class ConnectionMetaData {
		public final DatabaseConnection connection;
		private final long expiresMillis;
		private long lastUsed;

		public ConnectionMetaData(DatabaseConnection connection, long maxConnectionAgeMillis) {
			this.connection = connection;
			long now = System.currentTimeMillis();
			if (maxConnectionAgeMillis > Long.MAX_VALUE - now) {
				this.expiresMillis = Long.MAX_VALUE;
			} else {
				this.expiresMillis = now + maxConnectionAgeMillis;
			}
			this.lastUsed = now;
		}

		public boolean isExpired(long now) {
			return (expiresMillis <= now);
		}

		public long getLastUsed() {
			return lastUsed;
		}

		public void noteUsed() {
			this.lastUsed = System.currentTimeMillis();
		}

		@Override
		public String toString() {
			return "#" + hashCode();
		}
	}

	/**
	 * Tester thread that checks the connections that we have queued to make sure they are still good.
	 */
	private class ConnectionTester extends Thread {

		// class field to reduce gc
		private Set<ConnectionMetaData> testedSet = new HashSet<ConnectionMetaData>();

		@Override
		public void run() {
			while (checkConnectionsEveryMillis > 0) {
				try {
					Thread.sleep(checkConnectionsEveryMillis);
					if (!testConnections()) {
						return;
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					// quit if we've been interrupted
					return;
				}
			}
		}

		/**
		 * Test the connections, returning true if we should continue.
		 */
		private boolean testConnections() {
			// clear our tested set
			testedSet.clear();
			long now = System.currentTimeMillis();

			ConnectionMetaData connMetaData = null;
			boolean closeLast = false;
			while (true) {
				testLoopCount++;
				synchronized (lock) {
					if (closeLast) {
						if (connMetaData != null) {
							closeConnectionQuietly(connMetaData);
							connMetaData = null;
						}
						closeLast = false;
					}
					if (connFreeList == null) {
						// we're closed
						return false;
					}
					// add a tested connection back into the free-list
					if (connMetaData != null) {
						// we do this so we don't have to double lock in the loop
						connFreeList.add(connMetaData);
					}
					if (connFreeList.isEmpty()) {
						// nothing to do, return to sleep and go again
						return true;
					}

					connMetaData = connFreeList.get(0);
					if (testedSet.contains(connMetaData)) {
						// we are done if we've tested it before on this pass
						return true;
					}
					// otherwise, take the first one off the list
					connMetaData = connFreeList.remove(0);

					// see if it is expires so it can be closed immediately
					if (connMetaData.isExpired(now)) {
						// close expired connection
						closeConnectionQuietly(connMetaData);
						// don't return the connection to the free list
						connMetaData = null;
						continue;
					}
				}

				if (testConnection(connMetaData)) {
					testedSet.add(connMetaData);
				} else {
					// we close this inside of the synchronized block
					closeLast = true;
				}
			}
		}
	}
}
