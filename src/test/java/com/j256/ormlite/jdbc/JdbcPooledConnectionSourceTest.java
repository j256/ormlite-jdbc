package com.j256.ormlite.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DatabaseTypeUtils;
import com.j256.ormlite.support.DatabaseConnection;

public class JdbcPooledConnectionSourceTest {

	private static final String DEFAULT_DATABASE_URL = "jdbc:h2:mem:ormlite";

	@Test
	public void testBasic() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection();
			DatabaseConnection conn2 = pooled.getReadWriteConnection();
			assertNotSame(conn1, conn2);
			pooled.releaseConnection(conn2);
			DatabaseConnection conn3 = pooled.getReadWriteConnection();
			assertNotSame(conn1, conn3);
			assertSame(conn2, conn3);
			DatabaseConnection conn4 = pooled.getReadWriteConnection();
			assertNotSame(conn1, conn4);
			assertNotSame(conn3, conn4);
			pooled.releaseConnection(conn1);
			pooled.releaseConnection(conn3);
			DatabaseConnection conn5 = pooled.getReadOnlyConnection();
			assertSame(conn1, conn5);
			DatabaseConnection conn6 = pooled.getReadOnlyConnection();
			assertSame(conn3, conn6);
			conn1.close();
			conn2.close();
			conn3.close();
			conn4.close();
			conn5.close();
			conn6.close();
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testTransaction() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			pooled.setUsesTransactions(true);
			DatabaseConnection conn1 = pooled.getReadOnlyConnection();
			DatabaseConnection conn2 = pooled.getReadWriteConnection();
			pooled.saveSpecialConnection(conn2);
			DatabaseConnection conn3 = pooled.getReadWriteConnection();
			assertSame(conn2, conn3);
			pooled.releaseConnection(conn3);
			pooled.releaseConnection(conn1);
			DatabaseConnection conn4 = pooled.getReadWriteConnection();
			assertSame(conn2, conn4);
			pooled.releaseConnection(conn4);
			pooled.clearSpecialConnection(conn2);
			DatabaseConnection conn5 = pooled.getReadWriteConnection();
			assertSame(conn1, conn5);
			conn1.close();
			conn2.close();
			conn3.close();
			conn4.close();
			conn5.close();
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testMaxFree() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			pooled.setMaxConnectionsFree(2);
			DatabaseConnection conn1 = pooled.getReadWriteConnection();
			DatabaseConnection conn2 = pooled.getReadWriteConnection();
			DatabaseConnection conn3 = pooled.getReadWriteConnection();
			pooled.releaseConnection(conn1);
			pooled.releaseConnection(conn2);
			pooled.releaseConnection(conn3);
			DatabaseConnection conn4 = pooled.getReadWriteConnection();
			// not conn1 which should be closed
			assertSame(conn2, conn4);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testReleaseAfterClose() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		DatabaseConnection conn1;
		try {
			conn1 = pooled.getReadOnlyConnection();
		} finally {
			pooled.close();
		}
		pooled.releaseConnection(conn1);
	}

	@Test
	public void testMaxAge() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			long maxAge = 500;
			pooled.setMaxConnectionAgeMillis(maxAge);
			DatabaseConnection conn1 = pooled.getReadOnlyConnection();
			pooled.releaseConnection(conn1);
			Thread.sleep(maxAge + 1);
			DatabaseConnection conn2 = pooled.getReadOnlyConnection();
			assertNotSame(conn1, conn2);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testMaxAgeMaxValue() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			pooled.setMaxConnectionAgeMillis(Long.MAX_VALUE);
			DatabaseConnection conn1 = pooled.getReadOnlyConnection();
			pooled.releaseConnection(conn1);
		} finally {
			pooled.close();
		}
	}

	@Test(expected = SQLException.class)
	public void testCloseNoInit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		pooled.close();
	}

	@Test(expected = SQLException.class)
	public void testGetReadWriteNoInit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		try {
			pooled.getReadWriteConnection();
		} finally {
			pooled.close();
		}
	}

	@Test(expected = SQLException.class)
	public void testReleaseNoInit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		try {
			pooled.releaseConnection(null);
		} finally {
			pooled.close();
		}
	}

	@Test(expected = SQLException.class)
	public void testSaveTransaction() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		try {
			pooled.saveSpecialConnection(null);
		} finally {
			pooled.close();
		}
	}

	@Test(expected = SQLException.class)
	public void testClearTransaction() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		try {
			pooled.clearSpecialConnection(null);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testUsageCounts() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection();
			assertEquals(1, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(1, pooled.getMaxConnectionsInUse());
			assertEquals(1, pooled.getCurrentConnectionsManaged());
			assertEquals(1, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(1, pooled.getMaxConnectionsInUse());
			assertEquals(1, pooled.getCurrentConnectionsManaged());
			DatabaseConnection conn2 = pooled.getReadOnlyConnection();
			assertEquals(2, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(2, pooled.getMaxConnectionsInUse());
			assertEquals(2, pooled.getCurrentConnectionsManaged());
			pooled.releaseConnection(conn2);
			pooled.releaseConnection(conn1);
			assertEquals(2, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(2, pooled.getMaxConnectionsInUse());
			assertEquals(2, pooled.getCurrentConnectionsManaged());
		} finally {
			pooled.close();
		}
		assertEquals(2, pooled.getCloseCount());
		assertEquals(2, pooled.getMaxConnectionsInUse());
		assertEquals(0, pooled.getCurrentConnectionsManaged());
	}

	@Test
	public void testReleaseAlreadyClosed() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection();
			conn1.close();
			pooled.releaseConnection(conn1);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testReleaseUnknown() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			JdbcDatabaseConnection conn1 =
					new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			pooled.releaseConnection(conn1);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testReleaseAlreadyClosedUnknown() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			JdbcDatabaseConnection conn1 =
					new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			conn1.close();
			pooled.releaseConnection(conn1);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testConstructors() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());

		pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL, null, null);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());

		DatabaseType databaseType = DatabaseTypeUtils.createDatabaseType(DEFAULT_DATABASE_URL);
		pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL, databaseType);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());
		assertSame(databaseType, pooled.getDatabaseType());

		pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL, null, null, databaseType);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());
		assertSame(databaseType, pooled.getDatabaseType());
	}

	@Test
	public void testNestedConnection() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			JdbcDatabaseConnection conn1 =
					new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			pooled.saveSpecialConnection(conn1);
			pooled.saveSpecialConnection(conn1);
			pooled.releaseConnection(conn1);
			pooled.releaseConnection(conn1);
		} finally {
			pooled.close();
		}
	}
	
	@Test(expected = IllegalStateException.class)
	public void testSaveOtherConnection() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			JdbcDatabaseConnection conn1 =
				new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			JdbcDatabaseConnection conn2 =
				new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			pooled.saveSpecialConnection(conn1);
			pooled.saveSpecialConnection(conn2);
		} finally {
			pooled.close();
		}
	}
	
	@Test
	public void testClearOtherConnection() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			JdbcDatabaseConnection conn1 =
				new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			JdbcDatabaseConnection conn2 =
				new JdbcDatabaseConnection(DriverManager.getConnection(DEFAULT_DATABASE_URL));
			pooled.saveSpecialConnection(conn1);
			pooled.clearSpecialConnection(conn2);
		} finally {
			pooled.close();
		}
	}

}
