package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.j256.ormlite.BaseCoreTest;
import com.j256.ormlite.db.H2DatabaseType;
import com.j256.ormlite.support.DatabaseConnection;

public class JdbcConnectionSourceTest extends BaseCoreTest {

	@Test
	public void testSimpleDataSource() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		String url = "jdbc:h2:mem:baz";
		sds.setUrl(url);
		assertEquals(url, sds.getUrl());
		sds.initialize();
		sds.close();
	}

	@Test
	public void testSimpleDataSourceString() throws Exception {
		String url = "jdbc:h2:mem:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url);
		assertEquals(url, sds.getUrl());
		sds.close();
	}

	@Test
	public void testSimpleDataSourceStringStringString() throws Exception {
		String username = "user";
		String password = "_secret";
		String url = "jdbc:h2:mem:ormlite-up;USER=" + username + ";PASSWORD=" + password;
		JdbcConnectionSource sds = new JdbcConnectionSource(url, username, password);
		assertNotNull(sds.getReadOnlyConnection(null));
		sds.close();
	}

	@Test
	public void testGetConnection() throws Exception {
		Connection conn = createMock(Connection.class);
		Driver driver = createMock(Driver.class);
		String url = "jdbc:bar:mem:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertNotNull(sds.getReadOnlyConnection(null));
			sds.close();
			verify(driver);
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testGetConnectionUserPassSetters() throws Exception {
		String username = "user";
		String password = "_secret";
		String url = "jdbc:h2:mem:ormlite-up;USER=" + username + ";PASSWORD=" + password;
		JdbcConnectionSource sds = new JdbcConnectionSource(url);
		sds.setUsername(username);
		sds.setPassword(password);
		assertNotNull(sds.getReadOnlyConnection(null));
		sds.close();
	}

	@Test(expected = SQLException.class)
	public void testGetConnectionNull() throws Exception {
		Driver driver = createMock(Driver.class);
		Properties props = new Properties();
		String url = "jdbc:bar:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(eq(url), eq(props))).andReturn(null);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			sds.getReadOnlyConnection(null);
			sds.close();
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testClose() throws Exception {
		Connection conn = createMock(Connection.class);
		conn.setAutoCommit(true);
		conn.close();
		Driver driver = createMock(Driver.class);
		String url = "jdbc:bar:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver, conn);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertNotNull(sds.getReadOnlyConnection(null));
			sds.close();
			verify(driver, conn);
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test(expected = SQLException.class)
	public void testInitNoUrl() throws Exception {
		JdbcConnectionSource cs = new JdbcConnectionSource();
		cs.initialize();
		cs.close();
	}

	@Test(expected = SQLException.class)
	public void testConnectionClosed() throws Exception {
		Connection conn = createMock(Connection.class);
		conn.setAutoCommit(true);
		expect(conn.isClosed()).andReturn(true);
		Driver driver = createMock(Driver.class);
		String url = "jdbc:bar:baz";
		expect(driver.acceptsURL(url)).andReturn(true);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver, conn);
		DriverManager.registerDriver(driver);
		try {
			JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
			assertNotNull(sds.getReadOnlyConnection(null));
			sds.getReadOnlyConnection(null);
			sds.close();
			fail("Should not get here");
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testSpringWiring() throws Exception {
		String url = "jdbc:h2:mem:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.setUrl(url);
		sds.initialize();
		sds.close();
	}

	@Test(expected = IOException.class)
	public void testCloseBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.close();
	}

	@Test(expected = SQLException.class)
	public void testGetReadOnlyConnectionBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.getReadOnlyConnection(null);
		sds.close();
	}

	@Test(expected = SQLException.class)
	public void testGetReadWriteConnectionBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.getReadWriteConnection(null);
		sds.close();
	}

	@Test(expected = SQLException.class)
	public void testReleaseConnectionBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.releaseConnection(null);
		sds.close();
	}

	@Test(expected = IllegalStateException.class)
	public void testGetDatabaseTypeBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.getDatabaseType();
		sds.close();
	}

	@Test
	public void testDoubleInit() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		sds.initialize();
		sds.close();
	}

	@Test
	public void testSaveAndClear() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		DatabaseConnection conn1 = sds.getReadOnlyConnection(null);
		DatabaseConnection conn2 = sds.getReadOnlyConnection(null);
		assertSame(conn1, conn2);
		sds.saveSpecialConnection(conn1);
		sds.clearSpecialConnection(conn1);
		sds.releaseConnection(conn1);
		sds.close();
	}

	@Test
	public void testSetDatabaseType() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.setUrl("jdbc:h2:mem:baz");
		sds.setDatabaseType(new H2DatabaseType());
		sds.initialize();
		assertTrue(sds.getDatabaseType() instanceof H2DatabaseType);
		sds.close();
	}

	@Test
	public void testIsOpen() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		// no get connection yet
		assertFalse(sds.isOpen(null));
		sds.releaseConnection(sds.getReadOnlyConnection(null));
		assertTrue(sds.isOpen(null));
		sds.close();
		assertFalse(sds.isOpen(null));
	}
}
