package com.j256.ormlite.jdbc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.j256.ormlite.BaseOrmLiteCoreTest;
import com.j256.ormlite.db.H2DatabaseType;
import com.j256.ormlite.support.DatabaseConnection;

public class JdbcConnectionSourceTest extends BaseOrmLiteCoreTest {

	@Test
	public void testSimpleDataSource() {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		String url = "foo:bar:baz";
		sds.setUrl(url);
		assertEquals(url, sds.getUrl());
	}

	@Test
	public void testSimpleDataSourceString() throws Exception {
		String url = "jdbc:h2:mem:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url);
		assertEquals(url, sds.getUrl());
	}

	@Test
	public void testSimpleDataSourceStringStringString() throws Exception {
		String username = "user";
		String password = "_secret";
		String url = "jdbc:h2:mem:ormlite-up;USER=" + username + ";PASSWORD=" + password;
		JdbcConnectionSource sds = new JdbcConnectionSource(url, username, password);
		assertNotNull(sds.getReadOnlyConnection());
	}

	@Test
	public void testGetConnection() throws Exception {
		String url = "jdbc:bar:mem:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Connection conn = createMock(Connection.class);
		Driver driver = createMock(Driver.class);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			assertNotNull(sds.getReadOnlyConnection());
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
		assertNotNull(sds.getReadOnlyConnection());
	}

	@Test(expected = SQLException.class)
	public void testGetConnectionNull() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Driver driver = createMock(Driver.class);
		Properties props = new Properties();
		expect(driver.connect(eq(url), eq(props))).andReturn(null);
		replay(driver);
		DriverManager.registerDriver(driver);
		try {
			sds.getReadOnlyConnection();
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test
	public void testClose() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Connection conn = createMock(Connection.class);
		conn.close();
		Driver driver = createMock(Driver.class);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		replay(conn);
		DriverManager.registerDriver(driver);
		try {
			assertNotNull(sds.getReadOnlyConnection());
			sds.close();
			verify(driver);
			verify(conn);
		} finally {
			DriverManager.deregisterDriver(driver);
		}
	}

	@Test(expected = SQLException.class)
	public void testInitNoUrl() throws Exception {
		new JdbcConnectionSource().initialize();
	}

	@Test(expected = SQLException.class)
	public void testConnectionClosed() throws Exception {
		String url = "jdbc:bar:baz";
		JdbcConnectionSource sds = new JdbcConnectionSource(url, databaseType);
		Connection conn = createMock(Connection.class);
		expect(conn.isClosed()).andReturn(true);
		Driver driver = createMock(Driver.class);
		expect(driver.connect(isA(String.class), isA(Properties.class))).andReturn(conn);
		replay(driver);
		replay(conn);
		DriverManager.registerDriver(driver);
		try {
			assertNotNull(sds.getReadOnlyConnection());
			sds.getReadOnlyConnection();
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
	}

	@Test(expected = SQLException.class)
	public void testCloseBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.close();
	}

	@Test(expected = SQLException.class)
	public void testGetReadOnlyConnectionBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.getReadOnlyConnection();
	}

	@Test(expected = SQLException.class)
	public void testGetReadWriteConnectionBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.getReadWriteConnection();
	}

	@Test(expected = SQLException.class)
	public void testReleaseConnectionBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.releaseConnection(null);
	}

	@Test(expected = IllegalStateException.class)
	public void testGetDatabaseTypeBeforeInitialize() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.getDatabaseType();
	}

	@Test
	public void testDoubleInit() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		sds.initialize();
	}

	@Test
	public void testSaveAndClear() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource("jdbc:h2:mem:baz");
		DatabaseConnection conn1 = sds.getReadOnlyConnection();
		DatabaseConnection conn2 = sds.getReadOnlyConnection();
		assertSame(conn1, conn2);
		sds.saveSpecialConnection(conn1);
		sds.clearSpecialConnection(conn1);
		sds.releaseConnection(conn1);
	}
	
	@Test
	public void testSetDatabaseType() throws Exception {
		JdbcConnectionSource sds = new JdbcConnectionSource();
		sds.setUrl("jdbc:h2:mem:baz");
		sds.setDatabaseType(new H2DatabaseType());
		sds.initialize();
		assertTrue(sds.getDatabaseType() instanceof H2DatabaseType);
	}
}

