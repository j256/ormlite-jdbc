package com.j256.ormlite.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.db.DatabaseType;
import com.j256.ormlite.db.DatabaseTypeUtils;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.misc.IOUtils;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.table.TableUtils;

public class JdbcPooledConnectionSourceTest {

	private static final String DEFAULT_DATABASE_URL = "jdbc:h2:mem:ormlite";

	@Test
	public void testBasic() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			assertNotSame(conn1, conn2);
			pooled.releaseConnection(conn2);
			DatabaseConnection conn3 = pooled.getReadWriteConnection(null);
			assertNotSame(conn1, conn3);
			assertSame(conn2, conn3);
			DatabaseConnection conn4 = pooled.getReadWriteConnection(null);
			assertNotSame(conn1, conn4);
			assertNotSame(conn3, conn4);
			pooled.releaseConnection(conn1);
			pooled.releaseConnection(conn3);
			DatabaseConnection conn5 = pooled.getReadOnlyConnection(null);
			assertSame(conn1, conn5);
			DatabaseConnection conn6 = pooled.getReadOnlyConnection(null);
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
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			pooled.saveSpecialConnection(conn2);
			DatabaseConnection conn3 = pooled.getReadWriteConnection(null);
			assertSame(conn2, conn3);
			pooled.releaseConnection(conn3);
			pooled.releaseConnection(conn1);
			DatabaseConnection conn4 = pooled.getReadWriteConnection(null);
			assertSame(conn2, conn4);
			pooled.releaseConnection(conn4);
			pooled.clearSpecialConnection(conn2);
			DatabaseConnection conn5 = pooled.getReadWriteConnection(null);
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
			DatabaseConnection conn1 = pooled.getReadWriteConnection(null);
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			DatabaseConnection conn3 = pooled.getReadWriteConnection(null);
			pooled.releaseConnection(conn1);
			pooled.releaseConnection(conn2);
			pooled.releaseConnection(conn3);
			DatabaseConnection conn4 = pooled.getReadWriteConnection(null);
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
			conn1 = pooled.getReadOnlyConnection(null);
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
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			pooled.releaseConnection(conn1);
			Thread.sleep(maxAge + 1);
			DatabaseConnection conn2 = pooled.getReadOnlyConnection(null);
			assertNotSame(conn1, conn2);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testClosedConnction() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		long checkEveryMillis = 250;
		pooled.setCheckConnectionsEveryMillis(checkEveryMillis);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			assertEquals(0, pooled.getReleaseCount());
			assertEquals(0, pooled.getCurrentConnectionsFree());
			pooled.releaseConnection(conn1);
			assertEquals(1, pooled.getReleaseCount());
			assertEquals(1, pooled.getCurrentConnectionsFree());

			// wait a bit
			Thread.sleep(checkEveryMillis + 50);

			DatabaseConnection conn2 = pooled.getReadOnlyConnection(null);
			// should get the same connection
			assertSame(conn1, conn2);
			pooled.releaseConnection(conn2);
			assertEquals(2, pooled.getReleaseCount());
			assertEquals(1, pooled.getCurrentConnectionsFree());

			// close it behind the scenes
			conn2.close();

			// wait a bit
			Thread.sleep(checkEveryMillis + 50);
			DatabaseConnection conn3 = pooled.getReadOnlyConnection(null);
			// now it should be different
			assertTrue(conn3 != conn1);
			assertEquals(0, pooled.getCurrentConnectionsFree());

			Thread.sleep(200);
			assertTrue("loop counter is " + pooled.getTestLoopCount(), pooled.getTestLoopCount() < 100);

		} finally {
			pooled.close();
		}
	}

	@Test
	public void testMaxAgeMaxValue() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			pooled.setMaxConnectionAgeMillis(Long.MAX_VALUE);
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			pooled.releaseConnection(conn1);
		} finally {
			pooled.close();
		}
	}

	@Test(expected = IOException.class)
	public void testCloseNoInit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		pooled.close();
	}

	@Test(expected = SQLException.class)
	public void testGetReadWriteNoInit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		try {
			pooled.getReadWriteConnection(null);
		} finally {
			IOUtils.closeQuietly(pooled);
		}
	}

	@Test(expected = SQLException.class)
	public void testReleaseNoInit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		try {
			pooled.releaseConnection(null);
		} finally {
			IOUtils.closeQuietly(pooled);
		}
	}

	@Test(expected = SQLException.class)
	public void testSaveTransaction() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		pooled.initialize();
		try {
			pooled.saveSpecialConnection(null);
		} finally {
			IOUtils.closeQuietly(pooled);
		}
	}

	@Test(expected = SQLException.class)
	public void testClearTransaction() throws SQLException {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource();
		pooled.initialize();
		try {
			pooled.clearSpecialConnection(null);
		} finally {
			IOUtils.closeQuietly(pooled);
		}
	}

	@Test
	public void testUsageCounts() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			assertEquals(1, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(1, pooled.getMaxConnectionsEverUsed());
			assertEquals(1, pooled.getCurrentConnectionsManaged());
			assertEquals(1, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(1, pooled.getMaxConnectionsEverUsed());
			assertEquals(1, pooled.getCurrentConnectionsManaged());
			DatabaseConnection conn2 = pooled.getReadOnlyConnection(null);
			assertEquals(2, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(2, pooled.getMaxConnectionsEverUsed());
			assertEquals(2, pooled.getCurrentConnectionsManaged());
			pooled.releaseConnection(conn2);
			pooled.releaseConnection(conn1);
			assertEquals(2, pooled.getOpenCount());
			assertEquals(0, pooled.getCloseCount());
			assertEquals(2, pooled.getMaxConnectionsEverUsed());
			assertEquals(2, pooled.getCurrentConnectionsManaged());
		} finally {
			pooled.close();
		}
		assertEquals(2, pooled.getCloseCount());
		assertEquals(2, pooled.getMaxConnectionsEverUsed());
		assertEquals(0, pooled.getCurrentConnectionsManaged());
	}

	@Test
	public void testReleaseAlreadyClosed() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
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
		pooled.close();

		pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL, null, null);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());
		pooled.close();

		DatabaseType databaseType = DatabaseTypeUtils.createDatabaseType(DEFAULT_DATABASE_URL);
		pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL, databaseType);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());
		assertSame(databaseType, pooled.getDatabaseType());
		pooled.close();

		pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL, null, null, databaseType);
		assertEquals(DEFAULT_DATABASE_URL, pooled.getUrl());
		assertSame(databaseType, pooled.getDatabaseType());
		pooled.close();
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

	@Test(expected = SQLException.class)
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

	@Test(expected = SQLException.class)
	public void testTestConnectionThatWasClosed() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		String pingStatement = pooled.getDatabaseType().getPingStatement();
		try {
			DatabaseConnection conn1 = pooled.getReadWriteConnection(null);
			conn1.queryForLong(pingStatement);
			pooled.releaseConnection(conn1);
			// close it behind the pool's back, bad dog
			conn1.close();
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			assertSame(conn1, conn2);
			conn2.queryForLong(pingStatement);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testAuthTestConnection() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		long delay = 100;
		pooled.setCheckConnectionsEveryMillis(delay);
		String pingStatement = pooled.getDatabaseType().getPingStatement();
		try {
			DatabaseConnection conn1 = pooled.getReadWriteConnection(null);
			conn1.queryForLong(pingStatement);
			pooled.releaseConnection(conn1);
			// make it test ok once
			Thread.sleep(delay + 50);
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			assertSame(conn1, conn2);
			conn2.queryForLong(pingStatement);
			pooled.releaseConnection(conn2);
			// close it behind the pool's back, bad dog
			conn2.close();
			// now it should find out that the connection is bad and pull it
			Thread.sleep(delay + 50);
			DatabaseConnection conn3 = pooled.getReadWriteConnection(null);
			assertNotSame(conn2, conn3);
			// this should work
			conn3.queryForLong(pingStatement);
			pooled.releaseConnection(conn3);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testAuthTestConnectionExpired() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		long delay = 100;
		pooled.setCheckConnectionsEveryMillis(delay);
		pooled.setMaxConnectionAgeMillis(delay);
		String pingStatement = pooled.getDatabaseType().getPingStatement();
		try {
			DatabaseConnection conn1 = pooled.getReadWriteConnection(null);
			conn1.queryForLong(pingStatement);
			pooled.releaseConnection(conn1);
			// make it test ok once
			Thread.sleep(delay * 2);
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			assertNotSame(conn1, conn2);
			conn2.queryForLong(pingStatement);
			pooled.releaseConnection(conn2);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testTestClosedConnectionWithTesting() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		String pingStatement = pooled.getDatabaseType().getPingStatement();
		pooled.setTestBeforeGet(true);
		try {
			DatabaseConnection conn1 = pooled.getReadWriteConnection(null);
			conn1.queryForLong(pingStatement);
			pooled.releaseConnection(conn1);
			// close it behind the pool's back, bad dog
			conn1.close();
			DatabaseConnection conn2 = pooled.getReadWriteConnection(null);
			assertNotSame(conn1, conn2);
			conn2.queryForLong(pingStatement);
			pooled.releaseConnection(conn2);
			DatabaseConnection conn3 = pooled.getReadWriteConnection(null);
			assertSame(conn2, conn3);
			conn3.queryForLong(pingStatement);
			pooled.releaseConnection(conn3);
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testIsOpen() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			pooled.setUrl("jdbc:h2:mem:baz");
			assertTrue(pooled.isOpen(null));
			DatabaseConnection conn = pooled.getReadOnlyConnection(null);
			pooled.releaseConnection(conn);
			assertTrue(pooled.isOpen(null));
		} finally {
			pooled.close();
			assertFalse(pooled.isOpen(null));
		}
	}

	@Test
	public void testSetAutoCommit() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		try {
			DatabaseConnection conn1 = pooled.getReadOnlyConnection(null);
			conn1.setAutoCommit(false);
			pooled.releaseConnection(conn1);
			DatabaseConnection conn2 = pooled.getReadOnlyConnection(null);
			assertSame(conn1, conn2);
			assertTrue(conn2.isAutoCommit());
		} finally {
			pooled.close();
		}
	}

	@Test
	public void testConnectionRollback() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		Dao<Foo, Integer> dao = null;
		DatabaseConnection conn = null;
		try {
			TableUtils.createTable(pooled, Foo.class);
			dao = DaoManager.createDao(pooled, Foo.class);
			conn = dao.startThreadConnection();
			dao.setAutoCommit(conn, false);
			Foo foo = new Foo();
			assertEquals(1, dao.create(foo));
			assertNotNull(dao.queryForId(foo.id));
			dao.endThreadConnection(conn);
			assertNull(dao.queryForId(foo.id));
		} finally {
			TableUtils.dropTable(pooled, Foo.class, true);
			if (dao != null) {
				dao.endThreadConnection(conn);
			}
			pooled.close();
		}
	}

	protected static class Foo {
		@DatabaseField(generatedId = true)
		int id;
		@DatabaseField
		String stuff;

		public Foo() {
		}
	}
}
