package com.j256.ormlite.jdbc;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.sql.SQLException;

import org.junit.Test;

import com.j256.ormlite.support.DatabaseConnection;

public class JdbcPooledConnectionSourceTest {

	private static final String DEFAULT_DATABASE_URL = "jdbc:h2:mem:ormlite";

	@Test
	public void testBasic() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
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
		pooled.close();
	}

	@Test
	public void testTransaction() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		pooled.setUsesTransactions(true);
		DatabaseConnection conn1 = pooled.getReadOnlyConnection();
		DatabaseConnection conn2 = pooled.getReadWriteConnection();
		pooled.saveTransactionConnection(conn2);
		DatabaseConnection conn3 = pooled.getReadWriteConnection();
		assertSame(conn2, conn3);
		pooled.releaseConnection(conn3);
		pooled.releaseConnection(conn1);
		DatabaseConnection conn4 = pooled.getReadWriteConnection();
		assertSame(conn2, conn4);
		pooled.releaseConnection(conn4);
		pooled.clearTransactionConnection(conn2);
		DatabaseConnection conn5 = pooled.getReadWriteConnection();
		assertSame(conn1, conn5);
		conn1.close();
		conn2.close();
		conn3.close();
		conn4.close();
		conn5.close();
		pooled.close();
	}

	@Test
	public void testMaxFree() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
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
	}
	
	@Test
	public void testReleaseAfterClose() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		DatabaseConnection conn1 = pooled.getReadOnlyConnection();
		pooled.close();
		pooled.releaseConnection(conn1);
	}

	@Test
	public void testMaxAge() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		long maxAge = 500;
		pooled.setMaxConnectionAgeMillis(maxAge);
		DatabaseConnection conn1 = pooled.getReadOnlyConnection();
		pooled.releaseConnection(conn1);
		Thread.sleep(maxAge + 1);
		DatabaseConnection conn2 = pooled.getReadOnlyConnection();
		assertNotSame(conn1, conn2);
		pooled.close();
	}

	@Test
	public void testMaxAgeMaxValue() throws Exception {
		JdbcPooledConnectionSource pooled = new JdbcPooledConnectionSource(DEFAULT_DATABASE_URL);
		pooled.setMaxConnectionAgeMillis(Long.MAX_VALUE);
		DatabaseConnection conn1 = pooled.getReadOnlyConnection();
		pooled.releaseConnection(conn1);
		pooled.close();
	}

	@Test(expected = SQLException.class)
	public void testCloseNoInit() throws Exception {
		JdbcPooledConnectionSource pool = new JdbcPooledConnectionSource();
		pool.close();
	}

	@Test(expected = SQLException.class)
	public void testGetReadWriteNoInit() throws Exception {
		JdbcPooledConnectionSource pool = new JdbcPooledConnectionSource();
		pool.getReadWriteConnection();
	}

	@Test(expected = SQLException.class)
	public void testReleaseNoInit() throws Exception {
		JdbcPooledConnectionSource pool = new JdbcPooledConnectionSource();
		pool.releaseConnection(null);
	}

	@Test(expected = SQLException.class)
	public void testSaveTransaction() throws Exception {
		JdbcPooledConnectionSource pool = new JdbcPooledConnectionSource();
		pool.saveTransactionConnection(null);
	}

	@Test(expected = SQLException.class)
	public void testClearTransaction() throws Exception {
		JdbcPooledConnectionSource pool = new JdbcPooledConnectionSource();
		pool.clearTransactionConnection(null);
	}
}
